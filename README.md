# Transforms package

## Foundry dev tools configuration

See [here](https://emdgroup.github.io/foundry-dev-tools/configuration.html)

## Vscode launch.json config

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python Debugger: Current File",
      "type": "debugpy",
      "request": "launch",
      "module": "transforms.run",
      "args": ["${file}"],

      "console": "integratedTerminal"
    }
  ]
}
```
