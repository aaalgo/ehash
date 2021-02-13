#include <stdio.h>
#include <iostream>
#include <fstream>
#include <functional>
#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>
#include <glob/glob.hpp>
#include <CLI/CLI.hpp>
#define FMT_HEADER_ONLY
#include <fmt/format.h>
#define SPDLOG_FMT_EXTERNAL
#include <spdlog/spdlog.h>

namespace fs=std::filesystem;

using std::string;
using std::cout;
using std::endl;
using std::vector;

class sink: std::ofstream {
    std::mutex mutex;
public:
    using std::ofstream::ofstream;

    void add (char const *buf, size_t sz) {
        std::lock_guard<std::mutex> lock(mutex);
        write(buf, sz);
    }
};

string extract_key (char *buf, char *buf_end, int key, char delim) {
    // skip key - 1 items
    for (int i = 0; i < key; ++i) {
        while ((buf < buf_end) && (buf[0] != delim)) ++buf;
        if (buf >= buf_end) throw 0;
        buf += 1;
    }
    char *end = buf;
    while ((end < buf_end) && (end[0] != delim)) ++end;
    return string(buf, end);
}

void ensure_dir (fs::path const &dir) {
    if (fs::exists(dir)) {
        if (!fs::is_directory(dir)) {
            spdlog::error("{} exists but is not directory.", dir.string());
        }
    }
    else {
        fs::create_directories(dir);
    }
}

int main (int argc, char *argv[]) {
    int number = 100;
    char line_delim = '\n';
    char field_delim = '\t';
    size_t buf_size = 32 * 1024 * 1024;
    int key = 0;
    string batch;
    string loader;
    std::string output_format = "part-{:0>5d}";
    fs::path output_dir{"ehash_output"};
    vector<string> inputs;

    int log_level = spdlog::level::info;
    int dry = 0;

    {
        CLI::App cli{"ehash"};
        cli.add_option("-n,--number", number, "");
        cli.add_option("-d,--delimiter", field_delim, "field delimiter, default '\t'.");
        cli.add_option("--format", output_format, "output filename format");
        cli.add_option("-o,--output", output_dir, "output directory");
        cli.add_option("-b,--batch", batch, "batch");
        cli.add_option("-l,--loader", loader, "loader");
        cli.add_option("--buffer", buf_size, "buffer size");
        cli.add_option("input", inputs, "input directories");
        cli.add_flag_function("-v", [&log_level](int v){ log_level -= v;}, "verbose");
        cli.add_flag_function("-V", [&log_level](int v){ log_level += v;}, "less verbose");
        cli.add_flag("--dry", dry, "dry run.");
        CLI11_PARSE(cli, argc, argv);
    }
    
    if (log_level < 0) log_level = 0;
    spdlog::set_level(spdlog::level::level_enum(log_level));
    spdlog::debug("log level: {}", log_level);

    if (inputs.empty()) {
        spdlog::warn("no input found on command line, read from stdin.");
        string line;
        while (getline(std::cin, line)) {
            inputs.push_back(line);
        }
    }

    vector<string> jobs;
    for (auto const &input: inputs) {
        jobs.push_back(input);
        /*
        spdlog::debug("scanning {}", input_dir.string());
        if (fs::is_regular_file(input_dir)) {
            spdlog::debug("adding {}", input_dir.string());
            jobs.push_back(input_dir);
            continue;
        }
#if 0
        for (auto const &path: glob::rglob(input_dir.string() + "/**")) {
            spdlog::debug("testing {}", path.string());
            if (fs::is_regular_file(path)) {
                spdlog::debug("adding {}", path.string());
                jobs.push_back(path);
            }
        }
#else
        spdlog::error("{} is not a regular file.", input_dir.string());

#endif
*/
    }
    spdlog::info("{} input inputs found.", jobs.size());

    if (jobs.empty() || dry) return 0;

    ensure_dir(output_dir);
    vector<std::unique_ptr<sink>> sinks;
    for (int i = 0; i < number; ++i) {
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
        sinks.emplace_back(std::make_unique<sink>(output_path));
    }

    std::hash<std::string> hash_fn;
#pragma omp parallel schedule(dynamic, 1)
    for (int i = 0; i < jobs.size(); ++i) {
        auto const &path = jobs[i];
        spdlog::debug("processing {}", path);
        FILE *stream;
        if (loader.empty()) {
            stream = fopen(path.c_str(), "rb");
        }
        else {
            stream = popen("", "rb");
        }
        if (!stream) throw 0;
        char *lineptr = NULL;
        size_t len = 0;
        for (;;) {
            ssize_t r = getdelim(&lineptr, &len, line_delim, stream);
            if (r <= 0) break;
            string v = extract_key(lineptr, lineptr+r, key, field_delim);
            int part = hash_fn(v);
            sinks[part]->add(lineptr, r);
        }
        free(lineptr);
        if (loader.empty()) {
            fclose(stream);
        }
        else {
            pclose(stream);
        }
    }

    return 0;
}
