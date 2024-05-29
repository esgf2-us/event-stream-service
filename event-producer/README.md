# A prototype source of publish events

Use `settings.py` to specify a function that takes item metadata, converts them into a event message and sends to an event stream.
The default `settings.py` points to `producer.stdout` that prints an event message to stdout.

To generate publish events corresponding to a set of datasets, run:

```
python generate_events.py --path <path_to_a_directory_with_datasets>
```
for example
```
python generate_events.py --path CMIP6/AerChemMIP/AS-RCEC/TaiESM1/hist-piNTCF/r1i1p1f1
```
The path in the example has 167 datasets. You can use shorter paths to generate more publish events.
