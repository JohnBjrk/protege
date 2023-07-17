# protege

**NOTE** This is a work in progress which has not yet been published to hex.

A Gleam library for running OTP processes with overload protection for received messages.

This was initially built to support the gleam-logger `glimt`, but moved to a separate repository to make it available to other projects in need of a similar mechanism.

The overload mechanism is based on the overload protection for the erlang-logger. See ([Protecting the Handler from Overload](https://www.erlang.org/doc/apps/kernel/logger_chapter.html#protecting-the-handler-from-overload)) for details about the mechanism and the meaning of the different parameters.

See `demo.gleam` for an example on how to use the library.

## Running the demo

```sh
gleam run -m demo
```
