# mperf
mperf is a tool that can be used to test a limited number of Manta workloads.
Currently it doesn't do much other than upload variable-sized objects to Manta.

mperf is meant to be a tool that lets the user send many different workloads to
a Manta installation.

Currently mperf only supports one workload - an object-mostly write workload.
The user can specify the size of objects to upload, how many objects may be
queued in the client, and how long to wait between object uploads.

In any workload, the objects are simply zero-filled buffers, but that could be
easily changed in the future.

## Building
```
npm install
```

## Running

