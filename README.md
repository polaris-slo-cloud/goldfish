# GoldFish: Serverless Actors with Short-Term Memory State for the Edge-Cloud Continuum

GoldFish is a lightweight WebAssembly-based platform designed to execute serverless actors with short-term memory state. It optimizes serverless computing performance in the Edge-Cloud continuum by introducing new lifecycle and invocation models for actors. These models allow actors to retain state between executions and influence future message processing.

## Features

- **Short-Term Stateful Actors**: GoldFish enables actors to preserve state between executions without relying on external services, reducing latency and costs.
- **Serverless Invocation Model (SIM)**: Allows actors to influence the processing of future messages, improving efficiency in workflows and minimizing network overhead.
- **WebAssembly Isolation**: GoldFish uses WebAssembly (Wasm) for lightweight and secure isolation, making it ideal for resource-constrained environments like edge computing.
- **Efficient Data Exchange**: Optimizes data exchange latency by up to 92% and increases throughput by up to 10x compared to existing solutions.

## Architecture Overview

- **GoldFish Middleware**: Manages message routing and actor lifecycle, allowing actors to communicate directly without external storage.
- **Actor Dispatcher**: Handles the creation and management of actor instances, enabling efficient resource allocation.
- **WebAssembly-based Isolation**: Each actor runs in a Wasm sandbox for secure and efficient execution.


