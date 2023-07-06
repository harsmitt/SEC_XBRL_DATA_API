-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: db
-- Generation Time: Jul 06, 2023 at 11:49 AM
-- Server version: 8.0.32
-- PHP Version: 8.1.17

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `sec_xbrl`
--

-- --------------------------------------------------------

--
-- Table structure for table `cal`
--

CREATE TABLE `cal` (
  `file_name` varchar(255) NOT NULL,
  `adsh` varchar(20) NOT NULL,
  `grp` int NOT NULL,
  `arc` int NOT NULL,
  `negative` tinyint(1) NOT NULL,
  `ptag` varchar(256) NOT NULL,
  `pversion` varchar(20) NOT NULL,
  `ctag` varchar(256) NOT NULL,
  `cversion` varchar(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `dim`
--

CREATE TABLE `dim` (
  `file_name` varchar(255) NOT NULL,
  `dimhash` varchar(34) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `segments` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `segt` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `metadata_fields`
--

CREATE TABLE `metadata_fields` (
  `table_name` varchar(300) NOT NULL,
  `field_name` varchar(300) NOT NULL,
  `field_description` varchar(3000) DEFAULT NULL,
  `source` varchar(500) DEFAULT NULL,
  `field_type` varchar(1000) NOT NULL,
  `field_max_size` int DEFAULT NULL,
  `is_null` tinyint(1) NOT NULL,
  `is_primary_key` tinyint(1) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `mongo`
--

CREATE TABLE `mongo` (
  `adsh_key` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `value` json NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `num`
--

CREATE TABLE `num` (
  `file_name` varchar(255) NOT NULL,
  `adsh` varchar(20) NOT NULL,
  `tag` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `version` varchar(20) NOT NULL,
  `ddate` date NOT NULL,
  `qtrs` int NOT NULL,
  `uom` varchar(50) NOT NULL,
  `dimh` varchar(34) NOT NULL,
  `iprx` int NOT NULL,
  `value` float DEFAULT NULL,
  `footnote` varchar(512) DEFAULT NULL,
  `footlen` int DEFAULT NULL,
  `dimn` int DEFAULT NULL,
  `coreg` varchar(256) DEFAULT NULL,
  `durp` float DEFAULT NULL,
  `datp` int DEFAULT NULL,
  `dcml` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `pre`
--

CREATE TABLE `pre` (
  `file_name` varchar(255) NOT NULL,
  `adsh` varchar(20) NOT NULL,
  `report` int NOT NULL,
  `line` int NOT NULL,
  `stmt` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `inpth` tinyint(1) DEFAULT NULL,
  `tag` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs DEFAULT NULL,
  `version` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `prole` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `plabel` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `negating` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `ren`
--

CREATE TABLE `ren` (
  `file_name` varchar(255) NOT NULL,
  `adsh` varchar(20) NOT NULL,
  `report` int NOT NULL,
  `rfile` varchar(1) DEFAULT NULL,
  `menucat` varchar(2) DEFAULT NULL,
  `shortname` varchar(1500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `longname` varchar(2500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `roleuri` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `parentroleuri` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `parentreport` int DEFAULT NULL,
  `ultparentrpt` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `sub`
--

CREATE TABLE `sub` (
  `file_name` varchar(255) NOT NULL,
  `adsh` varchar(20) NOT NULL,
  `cik` int DEFAULT NULL,
  `name` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `sic` int DEFAULT NULL,
  `countryba` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `stprba` varchar(2) DEFAULT NULL,
  `cityba` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `zipba` varchar(10) DEFAULT NULL,
  `bas1` varchar(40) DEFAULT NULL,
  `bas2` varchar(40) DEFAULT NULL,
  `baph` varchar(20) DEFAULT NULL,
  `countryma` varchar(2) DEFAULT NULL,
  `stprma` varchar(2) DEFAULT NULL,
  `cityma` varchar(30) DEFAULT NULL,
  `zipma` varchar(10) DEFAULT NULL,
  `mas1` varchar(40) DEFAULT NULL,
  `mas2` varchar(40) DEFAULT NULL,
  `countryinc` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `stprinc` varchar(2) DEFAULT NULL,
  `ein` int DEFAULT NULL,
  `former` varchar(150) DEFAULT NULL,
  `changed` varchar(8) DEFAULT NULL,
  `afs` varchar(5) DEFAULT NULL,
  `wksi` tinyint(1) DEFAULT NULL,
  `fye` varchar(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `form` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `period` date DEFAULT NULL,
  `fy` year DEFAULT NULL,
  `fp` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `filed` date DEFAULT NULL,
  `accepted` datetime DEFAULT NULL,
  `prevrpt` tinyint(1) DEFAULT NULL,
  `detail` tinyint(1) DEFAULT NULL,
  `instance` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `nciks` int DEFAULT NULL,
  `aciks` varchar(120) DEFAULT NULL,
  `pubfloatusd` float DEFAULT NULL,
  `floatdate` date DEFAULT NULL,
  `floataxis` varchar(255) DEFAULT NULL,
  `floatmems` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `tag`
--

CREATE TABLE `tag` (
  `file_name` varchar(255) NOT NULL,
  `tag` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `version` varchar(20) NOT NULL,
  `custom` tinyint(1) DEFAULT NULL,
  `abstract` tinyint(1) DEFAULT NULL,
  `datatype` varchar(20) DEFAULT NULL,
  `iord` varchar(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `crdr` varchar(1) DEFAULT NULL,
  `tlabel` varchar(512) DEFAULT NULL,
  `doc` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `txt`
--

CREATE TABLE `txt` (
  `file_name` varchar(255) NOT NULL,
  `adsh` varchar(20) NOT NULL,
  `tag` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs NOT NULL,
  `version` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `ddate` date NOT NULL,
  `qtrs` int NOT NULL,
  `iprx` int NOT NULL,
  `lang` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `dcml` int DEFAULT NULL,
  `durp` float DEFAULT NULL,
  `datp` int DEFAULT NULL,
  `dimh` varchar(34) NOT NULL,
  `dimn` int DEFAULT NULL,
  `coreg` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `ESCAPED` tinyint(1) DEFAULT NULL,
  `srclen` int DEFAULT NULL,
  `txtlen` int DEFAULT NULL,
  `footnote` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `footlen` int DEFAULT NULL,
  `context` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `value` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `mongo`
--
ALTER TABLE `mongo`
  ADD PRIMARY KEY (`adsh_key`);

--
-- Indexes for table `pre`
--
ALTER TABLE `pre`
  ADD PRIMARY KEY (`file_name`,`adsh`,`report`,`line`);

--
-- Indexes for table `ren`
--
ALTER TABLE `ren`
  ADD PRIMARY KEY (`file_name`,`adsh`,`report`);

--
-- Indexes for table `sub`
--
ALTER TABLE `sub`
  ADD PRIMARY KEY (`adsh`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
