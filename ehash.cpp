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

using std::string;
using std::vector;

namespace fs=std::filesystem;

class sink: std::ofstream {
    std::mutex mutex;
public:
    using std::ofstream::ofstream;

    void add (char const *buf, size_t sz) {
        std::lock_guard<std::mutex> lock(mutex);
        write(buf, sz);
    }
};

std::hash<std::string> HASH;

size_t hash_key (char *buf, char *buf_end, int key, char delim) {
    // skip key - 1 items
    for (int i = 0; i < key; ++i) {
        while ((buf < buf_end) && (buf[0] != delim)) ++buf;
        if (buf >= buf_end) throw 0;
        buf += 1;
    }
    char *end = buf;
    while ((end < buf_end) && (end[0] != delim)) ++end;
    return HASH(string(buf, end));
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
        cli.add_option("-n,--number", number, "number of partitions (100)");
        cli.add_option("-d,--delimiter", field_delim, "field delimiter ('\t').");
        cli.add_option("--format", output_format, "output filename format (part-{:0>5d})");
        cli.add_option("-o,--output", output_dir, "output directory (ehash_out)");
        cli.add_option("-b,--batch", batch, "batch (default empty)");
        cli.add_option("-l,--loader", loader, "loader (default empty)");
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

    spdlog::info("{} inputs found.", inputs.size());

    if (inputs.empty() || dry) return 0;

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

#pragma omp parallel schedule(dynamic, 1)
    for (int i = 0; i < inputs.size(); ++i) {
        auto const &input = inputs[i];
        FILE *stream;
        if (loader.empty()) {
            spdlog::debug("loading {}", input);
            stream = fopen(input.c_str(), "r");
        }
        else {
            string cmd = fmt::format("{} {}", loader, input);
            spdlog::info("streaming {}", cmd);
            stream = popen(cmd.c_str(), "r");
        }
        if (!stream) throw 0;
        char *lineptr = NULL;
        size_t len = 0;
        for (;;) {
            ssize_t r = getdelim(&lineptr, &len, line_delim, stream);
            if (r <= 0) break;
            int part = hash_key(lineptr, lineptr+r, key, field_delim) % sinks.size();
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
