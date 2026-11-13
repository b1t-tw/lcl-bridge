import { configFrom, run } from './run.js';

const config = configFrom(process.env);
const controller = new AbortController();
const stop = () => controller.abort();
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
const log = message => console.error(`${new Date().toISOString()} ${message}`);
await run(config, { signal: controller.signal, log });
