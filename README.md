# SyncHero

An app for syncing files in remote sources to a local folder whilst extracting any archives.

## Description

By leveraging both Rclone and 7-Zip, this app will synchronise all files in the remote sources you configure to a local folder. Any archives are automatically extracted and recursively checked for other archives.
Effort is taken by SyncHero to delete extracted archives in order to minimise disk space usage, and also minimises downloads, extracts, and calls to remote backends by caching metadata.

## Getting Started

### Dependencies

Rclone: https://rclone.org/

7-Zip: https://www.7-zip.org/

### Installing

* Clone this repository to your machine
* Install both Rclone and 7-Zip

### Configuring

Please refer to the `EXAMPLE.config.json` file in this repository to configure your installation of SyncHero by creating your own `config.json` file in the root folder of your cloned copy of the repository. 

Note that the configuration under the `sources` section should refer to your configuration for Rclone. Please configure Rclone seperately by referring to the official Rclone documentation.


### Executing program

Simply run `main.py` with Python

Windows example:
```
python.exe .\main.py
```

## Help

SyncHero will log errors in the console as well as in log files in the root folder of your cloned copy of the repository.

## Authors

<a href="https://github.com/kristian-shaw/SyncHero/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=kristian-shaw/SyncHero"/>
</a>

## License

This project is licensed under the Unlicense License - see the LICENSE.md file for details

## Privacy Policy

Rest assured that none of your data will be collected by us. Please see the PRIVACY_POLICY.md file for details.
