---
active: true
iteration: 3
max_iterations: 1000
completion_promise: "WASM SQLITE DONE"
started_at: "2026-01-22T00:24:45Z"
---

Use the wasm build of flatbuffers in the ../flatbuffers folder to create a wasm sqlite build that uses flatbuffers as the backend storage mechanism.  This should be developed in C++, and compiled to wasm.  Use CMake and install emsdk locally and use that install (see ../tudat for an example). Streaming flatbuffers in should update separate b-trees, and the files should just be stacked flatbuffers that can be read with normal flatbuffer code.  Ingesting the special json schema or an IDL should create tables appropriately.  If anything in here is stupid or slow or a bad idea, let me know immediately.
