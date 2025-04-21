## Command Line Interface

```bash
# Full pipeline with custom location
python pipeline.py --location "Yaba, Nigeria" --table lagos_features

# Skip specific steps
python pipeline.py --skip-download --skip-clean  # DB only
python pipeline.py --skip-db  # Process data without loading

# Verbose output
python pipeline.py --verbose

# Help menu
python pipeline.py --help
```

Options:
- `--config`: Path to config file (default: settings.json)
- `--location`: Override OSM location name
- `--table`: Override PostGIS table name
- `--skip-*`: Skip specific pipeline steps
- `--verbose`: Show detailed progress