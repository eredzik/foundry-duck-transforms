# Transforms package

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
