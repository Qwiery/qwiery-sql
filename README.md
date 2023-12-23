# Qwiery SQL Adapter

This Qwiery adapter allows to use a Sql backend. It replaces the default JSON adapter and transparently uses the same Qwiery API.



```bash
 npm install @orbifold/sql
```

```js
import {Qwiery} from "@orbifold/dal";
import {Sql} from "@orbifold/sql";
// add the plugin to Qwiery
Qwiery.plugin(Sql);
const q = new Qwiery({
    // define which adapter to use (this replaces the default JSON adapter)
    adapters: ["sql"],
    // optional: replace the defaults (sqlite) to connect
    sql: {
        // the Sequelize options will be passed unchanged
        // https://sequelize.org/api/v7/interfaces/_sequelize_core.index.options
    }
});

```
