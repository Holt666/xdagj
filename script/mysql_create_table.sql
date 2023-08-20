CREATE TABLE `t_transaction_history` (
  `fid` int NOT NULL AUTO_INCREMENT,
  `faddress` varchar(64) NOT NULL,
  `faddresstype` tinyint NOT NULL,
  `fhash` varchar(64) NOT NULL,
  `famount` decimal(20,9) unsigned zerofill NOT NULL,
  `ftype` tinyint NOT NULL,
  `fremark` varchar(64) DEFAULT NULL,
  `ftime` datetime(3) NOT NULL,
  PRIMARY KEY (`fid`),
  UNIQUE KEY `id_UNIQUE` (`fid`),
  KEY `faddress_index` (`faddress`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci