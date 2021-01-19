# sumo-backend

## Steps to get up and running

1. Create a MySql database named `sumo`

2. Create the following tables:

```
 CREATE TABLE `rikishi` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `image` varchar(255) DEFAULT NULL,
  `name_ja` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=114 DEFAULT CHARSET=utf8;
```

```
CREATE TABLE `bout` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `east` varchar(255) DEFAULT NULL,
  `west` varchar(255) DEFAULT NULL,
  `east_rank` varchar(255) DEFAULT NULL,
  `west_rank` varchar(255) DEFAULT NULL,
  `winner` varchar(255) DEFAULT NULL,
  `winning_technique` varchar(255) DEFAULT NULL,
  `year` int(11) DEFAULT NULL,
  `day` int(11) DEFAULT NULL,
  `month` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4538 DEFAULT CHARSET=utf8;
```

3. Open `lein repl` from the root of this project

4. Run this in the repl to populate your database:
   `(load-file "./src/sumo_backend/process_json.clj")`

5. From the root of the project run: `lein ring server-headless`
   to start the api

## Prerequisites

You will need [Leiningen][] 2.0.0 or above installed.

[leiningen]: https://github.com/technomancy/leiningen

## Running

To start a web server for the application, run:

    lein ring server

## License

Copyright © 2021 FIXME
