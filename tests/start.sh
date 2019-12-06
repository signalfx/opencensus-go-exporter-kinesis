#!/usr/bin/env bash

create_stream() {
    sleep 2
    yarn run create
}

start_server() {
    yarn run start
}

create_stream & start_server

