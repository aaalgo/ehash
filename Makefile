CXXFLAGS += -std=c++17 -O3 -g -static \
	    -I3rd/glob/single_include \
	    -I3rd/fmt/include \
	    -I3rd/spdlog/include \
	    -I3rd/CLI11/include


.PHONY:	all


all:	ehash
