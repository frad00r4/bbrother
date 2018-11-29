-- MySQL dump

CREATE TABLE `users` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `login` VARCHAR(256) NOT NULL,
  `password` VARCHAR(256) NOT NULL,
  `stamp` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IX_login` (`login`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `trackers` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `user_id` INT(11) NOT NULL,
  `indefication` VARCHAR(50) NOT NULL,
  `authorisation` TEXT,
  `stamp` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`),
  KEY `IX_user` (`user_id`),
  KEY `IX_indefication` (`indefication`),
  CONSTRAINT `FK_trackers_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `geodata` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `tracker_id` INT(11) NOT NULL,
  `lat` FLOAT NOT NULL,
  `lon` FLOAT NOT NULL,
  `speed` FLOAT NOT NULL,
  `altitude` FLOAT NOT NULL,
  `stamp` TIMESTAMP NOT NULL,
  PRIMARY KEY (`id`),
  KEY `IX_tracker_id` (`tracker_id`),
  CONSTRAINT `FK_geodata_tracker` FOREIGN KEY (`tracker_id`) REFERENCES `trackers` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
