CXXFLAGS += -std=c++17 -O3 -g -pthread -static \
	    -I3rd/glob/single_include \
	    -I3rd/fmt/include \
	    -I3rd/spdlog/include \
	    -I3rd/CLI11/include \
	    -I3rd/cxxpool/src \

LDFLAGS += -static -Wl,--whole-archive -lpthread -Wl,--no-whole-archive


.PHONY:	all


all:	ehash
