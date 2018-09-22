# centreon-tmp

This program converts centreon data_bin SQL extract extract to IA/ML expected format.

## Build

Build with maven:

    mvn package

Artefact is an auto executable jar that maven builds in the `target` folder.\
To launch the programme:

## Launch

    java -jar generator-{version}.jar [metrics list file] [data file] [output file]

The program agregate informations in a map in memory so it could need quite a log of Heap, it could be needed to provided `Xmx` param to the JVM: `java -Xmx1g -jar ...`

## Description

Two input files are expected:

- **metrics list file** contains a list of all the metrics expected in the output file, each row in the file contains a metric label formated like that: `{host name}:{service name}:{metric name}`
- **data file** contains the data extracted from centreon database

The output file is in the format required by ML tool:

- CSV with header in the first row
- each row contains data of a single timestamp date
- first collumn contains the date (ISO-8601)
- other collumns contains metrics data

## Sources

There are two source classes:
- `com.centreon.TMPGenerator` is a first version for an old input format, now **depracated** 
- `com.centreon.TMPGenerator2` is the good one now
