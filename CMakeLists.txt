cmake_minimum_required(VERSION 3.19)
project(sqlite C)

set(CMAKE_C_STANDARD 99)

add_executable(sqlite main.c main.c meta_data.c meta_data.h meta_data.c input_buffer.c input_buffer.h const.c const.h)