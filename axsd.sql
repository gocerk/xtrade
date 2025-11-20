-- --------------------------------------------------------
-- Sunucu:                       127.0.0.1
-- Sunucu sürümü:                11.5.2-MariaDB - mariadb.org binary distribution
-- Sunucu İşletim Sistemi:       Win64
-- HeidiSQL Sürüm:               12.6.0.6765
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- traderprobot için veritabanı yapısı dökülüyor
CREATE DATABASE IF NOT EXISTS `traderprobot` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */;
USE `traderprobot`;

-- tablo yapısı dökülüyor traderprobot.admins
CREATE TABLE IF NOT EXISTS `admins` (
  `id` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

-- tablo yapısı dökülüyor traderprobot.bultende_olanlar
CREATE TABLE IF NOT EXISTS `bultende_olanlar` (
  `id` varchar(50) DEFAULT NULL,
  `time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

-- tablo yapısı dökülüyor traderprobot.license_keys
CREATE TABLE IF NOT EXISTS `license_keys` (
  `license_key` varchar(255) DEFAULT NULL,
  `time` varchar(50) DEFAULT NULL,
  `used` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

-- tablo yapısı dökülüyor traderprobot.signal_results
CREATE TABLE IF NOT EXISTS `signal_results` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `webhook_log_id` int(11) DEFAULT NULL,
  `coin` varchar(20) DEFAULT NULL,
  `action` varchar(10) DEFAULT NULL,
  `entry_price` decimal(20,8) DEFAULT NULL,
  `current_price` decimal(20,8) DEFAULT NULL,
  `stop_loss` decimal(20,8) DEFAULT NULL,
  `tp1_price` decimal(20,8) DEFAULT NULL,
  `tp2_price` decimal(20,8) DEFAULT NULL,
  `tp3_price` decimal(20,8) DEFAULT NULL,
  `profit_percentage` decimal(10,2) DEFAULT NULL,
  `status` enum('ACTIVE','COMPLETED','STOPPED') DEFAULT 'ACTIVE',
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `completed_at` timestamp NULL DEFAULT NULL,
  `tp_hit` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_status` (`status`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_webhook_log_id` (`webhook_log_id`),
  CONSTRAINT `signal_results_ibfk_1` FOREIGN KEY (`webhook_log_id`) REFERENCES `webhook_logs` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

-- tablo yapısı dökülüyor traderprobot.trading_limits
CREATE TABLE IF NOT EXISTS `trading_limits` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `max_leverage` int(11) NOT NULL DEFAULT 125,
  `max_usdt` decimal(15,2) NOT NULL DEFAULT 1000.00,
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

-- tablo yapısı dökülüyor traderprobot.user_api_keys
CREATE TABLE IF NOT EXISTS `user_api_keys` (
  `user_id` varchar(50) NOT NULL DEFAULT '',
  `api_key` text NOT NULL,
  `api_secret` text NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

-- tablo yapısı dökülüyor traderprobot.user_settings
CREATE TABLE IF NOT EXISTS `user_settings` (
  `user_id` varchar(50) NOT NULL DEFAULT '',
  `autotrade` tinyint(1) DEFAULT 0,
  `amount_per_trade` decimal(18,8) DEFAULT NULL,
  `profit_percent` decimal(5,2) DEFAULT NULL,
  `leverage` int(11) DEFAULT 3,
  `notifications` tinyint(1) DEFAULT 0,
  `stop_loss_percent` decimal(5,2) DEFAULT 1.00,
  `leveraged_amount` decimal(20,8) DEFAULT 0.00000000,
  `adjusted_stop_loss` decimal(10,8) DEFAULT 0.00000000,
  `adjusted_take_profit` decimal(10,8) DEFAULT 0.00000000,
  `profit_percent_1` decimal(5,2) DEFAULT 0.00,
  `profit_percent_2` decimal(5,2) DEFAULT 0.00,
  `adjusted_take_profit_1` decimal(5,2) DEFAULT NULL,
  `adjusted_take_profit_2` decimal(5,2) DEFAULT NULL,
  `margin_type` enum('isolated','crossed') DEFAULT 'isolated',
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

-- tablo yapısı dökülüyor traderprobot.webhook_logs
CREATE TABLE IF NOT EXISTS `webhook_logs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `action` varchar(10) DEFAULT NULL,
  `coin` varchar(20) DEFAULT NULL,
  `price` decimal(20,8) DEFAULT NULL,
  `timeframe` varchar(10) DEFAULT NULL,
  `technical_indicators` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`technical_indicators`)),
  `historical_prices` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`historical_prices`)),
  `ai_analysis` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`ai_analysis`)),
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2069 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Veri çıktısı seçilmemişti

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
