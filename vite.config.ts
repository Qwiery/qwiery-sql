import {defineConfig} from "vite";
import dts from "vite-plugin-dts";
import * as path from "path";
import {nodePolyfills} from 'vite-plugin-node-polyfills'

export default defineConfig({
    plugins: [
        nodePolyfills(),
        dts({
            rollupTypes: true,
            entryRoot: "src",
            tsconfigPath: path.join(__dirname, "tsconfig.json"),
        }),
    ],
    resolve: {
        alias: [
            {
                find: "~",
                replacement: path.resolve(__dirname, "./src"),
            },
        ],
    },
    build: {
        minify: false,
        rollupOptions: {
            external: [
                "fs",
                "lodash",
                "@faker-js/faker",
                "process",
                "neo4j-driver",
                "@orbifold/dal",
                "@orbifold/utils",
                "@orbifold/graphs",
                "@orbifold/projections",
                "sequelize",
                "sqlite3"
            ]
        },
        lib: {
            entry: path.resolve(__dirname, "src/index.ts"),
            fileName: "index",
            formats: ["cjs"],
        },
    },
});
