#include <stdio.h>
#include <atomic>
#include <iostream>
#include <fstream>
#include <functional>
#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>
#include <tuple>
#include <algorithm>
#include <chrono>
#include <CLI/CLI.hpp>
#include <cxxpool.h>
#define FMT_HEADER_ONLY
#include <fmt/format.h>
#define SPDLOG_FMT_EXTERNAL
#include <spdlog/spdlog.h>

using std::atomic;
using std::string;
using std::string_view;
using std::vector;

namespace fs=std::filesystem;

struct FileTask {
    fs::path path;
    ssize_t size;    // automatically determined file size

    FileTask (string const &p, bool sort): path(p) {
        if (sort) {
            size = fs::file_size(p);
        }
        else {
            size = -1;
        }
    }
};

class FileTasks: public vector<FileTask> {
    // used in std::sort to compare tasks
    static inline bool bigger_file_first (FileTask const &f1, FileTask const &f2) {
        return f1.size > f2.size;
    }
public:
    FileTasks (vector<string> const &paths, bool sort = true) {
        for (auto const &path: paths) {
            emplace_back(path, sort);
        }
        if (sort) {
            std::sort(begin(), end(), bigger_file_first);
        }
    }
};

void run_in_parallel (FileTasks const &tasks,
                     int threads,
                     std::function<void(FileTask const&)> worker) {
    spdlog::info("running {} tasks", tasks.size());
    cxxpool::thread_pool pool(threads);
    vector<std::future<void>> results;
    spdlog::debug("enqueue tasks");
    for (auto const &task: tasks) {
        results.push_back(std::move(pool.push(worker, task)));
    }
    spdlog::debug("waiting for tasks");
    cxxpool::wait(results.begin(), results.end());
}

class file_or_pipe {
protected:
    bool is_pipe;

    static inline bool is_fmt_string (string const &path) {
        auto off = path.find("{");
        if (off == path.npos) return false;
        off = path.find("}", off);
        if (off == path.npos) return false;
        return true;
    }

public:
    FILE *stream;
    file_or_pipe (string const &path, string const &loader, char const *mode) {
        if (loader.empty()) {
            spdlog::debug("loading {}", path);
            stream = fopen(path.c_str(), mode);
            is_pipe = false;
        }
        else {
            string cmd;
            if (is_fmt_string(loader)) {
                cmd = fmt::format(loader, path);
            }
            else {
                cmd = fmt::format("{} {}", loader, path);
            }
            spdlog::info("streaming {}", cmd);
            stream = popen(cmd.c_str(), mode);
            is_pipe = true;
        }
        if (!stream) throw 0;
    }
    ~file_or_pipe () {
        if (is_pipe) pclose(stream);
        else fclose(stream);
    }
};

class source: public file_or_pipe {
public:
    source (string const &path, string const &loader = "")
        : file_or_pipe(path, loader, "r") 
    {
    }
};

struct non_mutex {
    void lock () {}
    void unlock () {}
};

template <typename M=non_mutex>
class sink: protected file_or_pipe {
    M mutex;
public:
    sink (string const &path, string const &saver = "")
        : file_or_pipe(path, saver, "w") {
    }

    void write (char const *buf, size_t sz) {
        std::lock_guard<M> lock(mutex);
        fwrite(buf, 1, sz, stream);
    }

    ~sink () {
    }
};

typedef sink<std::mutex> sync_sink;
typedef sink<non_mutex> async_sink;

std::hash<string_view> HASH;

string_view extract_key (char *buf, char *buf_end, int key, char delim) {
    // skip key - 1 items
    for (int i = 0; i < key; ++i) {
        while ((buf < buf_end) && (buf[0] != delim)) ++buf;
        if (buf >= buf_end) throw 0;
        buf += 1;
    }
    char *end = buf;
    while ((end < buf_end) && (end[0] != delim)) ++end;
    return string_view(buf, end-buf);
}

void ensure_dir (fs::path const &dir) {
    if (fs::exists(dir)) {
        if (!fs::is_directory(dir)) {
            spdlog::error("{} exists but is not directory.", dir.native());
        }
    }
    else {
        fs::create_directories(dir);
    }
}

class Stats {
    atomic<size_t> lines = 0;
    atomic<size_t> bytes = 0;
    int interval = 0;
    size_t total_lines = 0;
    std::chrono::steady_clock::time_point begin;

    void report () const {
        double seconds = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin).count() / 1000000.0;
        double MB = 1.0 * bytes / 0x100000;
        if (total_lines > 0) {
            spdlog::info("loaded {} lines {} bytes in {:.1f}s at {:.1f} MB/s, {:.1f}% done.", lines, bytes, seconds, MB/seconds, 100.0 * lines/total_lines);
        }
        else {
            spdlog::info("loaded {} lines {} bytes in {:.1f}s at {:.1f} MB/s.", lines, bytes, seconds, MB/seconds);
        }
    }

public:
    Stats (int interval_, size_t total_): interval(interval_), total_lines(total_), begin(std::chrono::steady_clock::now()) {
    }

    ~Stats () {
        report();
    }

    void update (int l, ssize_t s) {
        size_t ls = lines.fetch_add(l);
        size_t ss = bytes.fetch_add(s);
        if (interval > 0) {
            ls += l;
            ss += s;
            if (ls % interval == 0) {
                report();
            }
        }
    }
};

int main (int argc, char *argv[]) {
    int partitions = 100;
    char line_delim = '\n';
    char field_delim = '\t';
    int key = 0;
    int threads = 0;
    string batch;
    string loader, combiner, reducer;
    string output_format = "part-{:0>5d}";
    fs::path output_dir{"ehash_output"};
    vector<string> inputs;

    int log_level = spdlog::level::info;
    int dry = 0;
    int sort = 0;
    int interval = 0;
    size_t progress_total_lines = 0;

    {
        CLI::App cli{"ehash"};
        cli.add_option("-p,--partitions", partitions, "number of partitions (100)");
        cli.add_option("-d,--delimiter", field_delim, "field delimiter ('\\t').");
        cli.add_option("--format", output_format, "output filename format (part-{:0>5d})");
        cli.add_option("-o,--output", output_dir, "output directory (ehash_out)");
        cli.add_option("-b,--batch", batch, "batch (default empty)");
        cli.add_option("-k,--key", key, "hash key");
        cli.add_option("-l,--loader", loader, "loader (default empty)");
        cli.add_option("-c,--combiner", combiner, "combiner (default empty)");
        cli.add_option("-r,--reducer", reducer, "reducer (default empty)");
        cli.add_option("-t,--threads", threads, "number of threads (0 for auto)");
        cli.add_option("-i,--interval", interval, "report every this number of lines (0)");
        cli.add_option("-n,--progress_total_lines", progress_total_lines, "");
        cli.add_option("input", inputs, "input directories");
        cli.add_flag_function("-v", [&log_level](int v){ log_level -= v;}, "verbose");
        cli.add_flag_function("-V", [&log_level](int v){ log_level += v;}, "less verbose");
        cli.add_flag("--dry", dry, "dry run.");
        cli.add_flag("--sort", sort, "");
        CLI11_PARSE(cli, argc, argv);
    }
    
    if (log_level < 0) log_level = 0;
    spdlog::set_level(spdlog::level::level_enum(log_level));
    spdlog::debug("log level: {}", log_level);

    if (reducer.size() && !sort) {
        spdlog::error("reducer specified but not sorting, abort.");
    }

    if (threads <= 0) {
        threads = std::thread::hardware_concurrency();
        if (threads <= 0) {
            threads = 4;
        }
        spdlog::info("using {} threads.", threads);
    }

    if (inputs.empty()) {
        spdlog::warn("no input found on command line, read from stdin.");
        string line;
        while (getline(std::cin, line)) {
            inputs.push_back(line);
        }
    }

    spdlog::info("aaalgo ehash, https://github.com/aaalgo/ehash");
    spdlog::info("{} inputs found.", inputs.size());

    if (inputs.empty() || dry) return 0;

    FileTasks input_tasks(inputs, loader.empty());

    ensure_dir(output_dir);
    vector<string> output_paths;
    vector<std::unique_ptr<sync_sink>> sinks;
    for (int i = 0; i < partitions; ++i) {
        string split = fmt::format(output_format, i);
        fs::path output_path;
        if (batch.empty()) {
            output_path = output_dir/split;
        }
        else {
            // create batch
            fs::path dir = output_dir/split;
            ensure_dir(dir);
            output_path = dir/batch;
        }
        output_paths.push_back(output_path.native());
        sinks.emplace_back(std::make_unique<sync_sink>(output_path.native(), combiner));
    }

    Stats stats(interval, progress_total_lines);
    run_in_parallel(input_tasks, threads, [&](FileTask const &task) {
        source source(task.path, loader);
        char *lineptr = NULL;
        size_t len = 0;
        for (;;) {
            ssize_t r = getdelim(&lineptr, &len, line_delim, source.stream);
            if (r <= 0) break;
            int part = HASH(extract_key(lineptr, lineptr+r, key, field_delim)) % sinks.size();
            sinks[part]->write(lineptr, r);
            stats.update(1, r);
        }
        free(lineptr);
    });

    sinks.clear();

    if (!sort) return 0;

    FileTasks sort_tasks(output_paths);
    run_in_parallel(sort_tasks, threads, [&](FileTask const &task) {
        vector<std::tuple<string_view, char *, size_t>> lines;
        spdlog::info("sorting {}", task.path.native());
        {
            FILE *stream = fopen(task.path.c_str(), "r");
            if (!stream) throw 0;
            for (;;) {
                char *lineptr = NULL;
                size_t len = 0;
                ssize_t r = getdelim(&lineptr, &len, line_delim, stream);
                if (r <= 0) break;
                lines.emplace_back(extract_key(lineptr, lineptr+r, key, field_delim), lineptr, r);
            }
            fclose(stream);
        }
        std::sort(lines.begin(), lines.end());
        async_sink sink(task.path.native(), reducer);
        for (auto &p: lines){
            auto [dummy, lineptr, len] = p;
            sink.write(lineptr, len);
            free(lineptr);
        }
    });

    return 0;
}
