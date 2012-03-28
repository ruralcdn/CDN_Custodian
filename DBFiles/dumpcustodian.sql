-- MySQL dump 10.13  Distrib 5.5.17, for Win32 (x86)
--
-- Host: localhost    Database: ruralcdn
-- ------------------------------------------------------
-- Server version	5.5.17

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `dbsync`
--

DROP TABLE IF EXISTS `dbsync`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dbsync` (
  `dbkey` varchar(100) NOT NULL DEFAULT '',
  `dbvalue` varchar(200) NOT NULL DEFAULT '',
  PRIMARY KEY (`dbkey`,`dbvalue`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dbsync`
--

LOCK TABLES `dbsync` WRITE;
/*!40000 ALTER TABLE `dbsync` DISABLE KEYS */;
/*!40000 ALTER TABLE `dbsync` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `downloadrequest`
--

DROP TABLE IF EXISTS `downloadrequest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `downloadrequest` (
  `contentId` varchar(50) NOT NULL DEFAULT '',
  PRIMARY KEY (`contentId`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `downloadrequest`
--

LOCK TABLES `downloadrequest` WRITE;
/*!40000 ALTER TABLE `downloadrequest` DISABLE KEYS */;
/*!40000 ALTER TABLE `downloadrequest` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dtndownrequest`
--

DROP TABLE IF EXISTS `dtndownrequest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dtndownrequest` (
  `contentid` varchar(50) NOT NULL DEFAULT '',
  PRIMARY KEY (`contentid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dtndownrequest`
--

LOCK TABLES `dtndownrequest` WRITE;
/*!40000 ALTER TABLE `dtndownrequest` DISABLE KEYS */;
/*!40000 ALTER TABLE `dtndownrequest` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dtnrequest`
--

DROP TABLE IF EXISTS `dtnrequest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dtnrequest` (
  `contentid` varchar(50) NOT NULL DEFAULT '',
  `uploadid` varchar(50) NOT NULL DEFAULT '',
  PRIMARY KEY (`contentid`,`uploadid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dtnrequest`
--

LOCK TABLES `dtnrequest` WRITE;
/*!40000 ALTER TABLE `dtnrequest` DISABLE KEYS */;
/*!40000 ALTER TABLE `dtnrequest` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `localdata`
--

DROP TABLE IF EXISTS `localdata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `localdata` (
  `contentid` varchar(50) NOT NULL DEFAULT '',
  PRIMARY KEY (`contentid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `localdata`
--

LOCK TABLES `localdata` WRITE;
/*!40000 ALTER TABLE `localdata` DISABLE KEYS */;
/*!40000 ALTER TABLE `localdata` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `status`
--

DROP TABLE IF EXISTS `status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `status` (
  `contentid` varchar(50) NOT NULL DEFAULT '',
  `type` int(11) NOT NULL DEFAULT '0',
  `totseg` int(11) DEFAULT NULL,
  `curseg` int(11) DEFAULT NULL,
  `off` int(11) DEFAULT NULL,
  `prefint` int(11) DEFAULT NULL,
  `prefrt` varchar(100) NOT NULL DEFAULT '',
  `appid` varchar(20) DEFAULT NULL,
  `sendmetadata` tinyint(1) DEFAULT NULL,
  `prefrtport` varchar(5) DEFAULT NULL,
  PRIMARY KEY (`contentid`,`type`,`prefrt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `status`
--

LOCK TABLES `status` WRITE;
/*!40000 ALTER TABLE `status` DISABLE KEYS */;
/*!40000 ALTER TABLE `status` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `synctable`
--

DROP TABLE IF EXISTS `synctable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `synctable` (
  `entity` varchar(50) DEFAULT NULL,
  `updated_till` int(11) DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `synctable`
--

LOCK TABLES `synctable` WRITE;
/*!40000 ALTER TABLE `synctable` DISABLE KEYS */;
/*!40000 ALTER TABLE `synctable` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `uploadeditem`
--

DROP TABLE IF EXISTS `uploadeditem`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `uploadeditem` (
  `dsid` varchar(50) NOT NULL,
  `csid` varchar(50) DEFAULT NULL,
  `flag` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`dsid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `uploadeditem`
--

LOCK TABLES `uploadeditem` WRITE;
/*!40000 ALTER TABLE `uploadeditem` DISABLE KEYS */;
/*!40000 ALTER TABLE `uploadeditem` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `uploadrequest`
--

DROP TABLE IF EXISTS `uploadrequest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `uploadrequest` (
  `contentid` varchar(50) NOT NULL DEFAULT '',
  `uploadid` varchar(50) NOT NULL DEFAULT '',
  `type` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`contentid`,`uploadid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `uploadrequest`
--

LOCK TABLES `uploadrequest` WRITE;
/*!40000 ALTER TABLE `uploadrequest` DISABLE KEYS */;
/*!40000 ALTER TABLE `uploadrequest` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `userdaemonloc`
--

DROP TABLE IF EXISTS `userdaemonloc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `userdaemonloc` (
  `usernode` varchar(50) NOT NULL DEFAULT '',
  `ipadd` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`usernode`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `userdaemonloc`
--

LOCK TABLES `userdaemonloc` WRITE;
/*!40000 ALTER TABLE `userdaemonloc` DISABLE KEYS */;
INSERT INTO `userdaemonloc` VALUES ('user1','14.98.205.6');
/*!40000 ALTER TABLE `userdaemonloc` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `userlocation`
--

DROP TABLE IF EXISTS `userlocation`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `userlocation` (
  `username` varchar(50) NOT NULL DEFAULT '',
  `usernode` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`username`),
  KEY `usernode` (`usernode`),
  CONSTRAINT `userlocation_ibfk_1` FOREIGN KEY (`usernode`) REFERENCES `userdaemonloc` (`usernode`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `userlocation`
--

LOCK TABLES `userlocation` WRITE;
/*!40000 ALTER TABLE `userlocation` DISABLE KEYS */;
INSERT INTO `userlocation` VALUES ('amitd','user1');
/*!40000 ALTER TABLE `userlocation` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `userupload`
--

DROP TABLE IF EXISTS `userupload`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `userupload` (
  `uploadeditem` varchar(50) NOT NULL DEFAULT '',
  `user` varchar(50) NOT NULL DEFAULT '',
  PRIMARY KEY (`uploadeditem`,`user`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `userupload`
--

LOCK TABLES `userupload` WRITE;
/*!40000 ALTER TABLE `userupload` DISABLE KEYS */;
/*!40000 ALTER TABLE `userupload` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2012-03-26 14:09:24
