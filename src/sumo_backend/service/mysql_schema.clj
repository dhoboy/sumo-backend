(ns sumo-backend.service.mysql-schema)
(require '[clojure.java.jdbc :as jdbc])

;; Mysql table definitions for tables used in this project

;;;;;;;;;;;;;;;;;;;;
;; rikishi table
;;;;;;;;;;;;;;;;;;;;

;; CREATE TABLE `rikishi` (
;;   `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
;;   `name` varchar(255) DEFAULT NULL,
;;   `image` varchar(255) DEFAULT NULL,
;;   `name_ja` varchar(255) DEFAULT NULL,
;;   PRIMARY KEY (`id`)
;; ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

(def rikishi-table
  (jdbc/create-table-ddl
    :rikishi
    [[:id "int(11)" :unsigned :not :null :auto_increment :primary :key]
     [:name "varchar(255)" :default :null]
     [:image "varchar(255)" :default :null]
     [:name_ja "varchar(255)" :default :null]]
    {:table-spec "ENGINE=InnoDB DEFAULT CHARSET=utf8"
     :conditional? true}))

;;;;;;;;;;;;;;;;;;;;
;; bout table
;;;;;;;;;;;;;;;;;;;;

;; CREATE TABLE `bout` (
;;   `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
;;   `east` varchar(255) DEFAULT NULL,
;;   `west` varchar(255) DEFAULT NULL,
;;   `east_rank` varchar(255) DEFAULT NULL,
;;   `east_rank_value` int(11) DEFAULT NULL,
;;   `west_rank` varchar(255) DEFAULT NULL,
;;   `west_rank_value` int(11) DEFAULT NULL,
;;   `winner` varchar(255) DEFAULT NULL,
;;   `loser` varchar(255) DEFAULT NULL,
;;   `is_playoff` tinyint(1) DEFAULT NULL,
;;   `technique` varchar(255) DEFAULT NULL,
;;   `technique_en` varchar(255) DEFAULT NULL,
;;   `technique_category` varchar(255) DEFAULT NULL,
;;   `year` int(11) DEFAULT NULL,
;;   `month` int(11) DEFAULT NULL,
;;   `day` int(11) DEFAULT NULL,
;;   PRIMARY KEY (`id`)
;; ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

(def bout-table
  (jdbc/create-table-ddl
    :bout
    [[:id "int(11)" :unsigned :not :null :auto_increment :primary :key]
     [:east "varchar(255)" :default :null]
     [:west "varchar(255)" :default :null]
     [:east_rank "varchar(255)" :default :null]
     [:east_rank_value "int(11)" :default :null]
     [:west_rank "varchar(255)" :default :null]
     [:west_rank_value "int(11)" :default :null]
     [:winner "varchar(255)" :default :null]
     [:loser "varchar(255)" :default :null]
     [:is_playoff "tinyint(1)" :default :null]
     [:technique "varchar(255)" :default :null]
     [:technique_en "varchar(255)" :default :null]
     [:technique_category "varchar(255)" :default :null]
     [:year "int(11)" :default :null]
     [:month "int(11)" :default :null]
     [:day "int(11)" :default :null]]
   {:table-spec "ENGINE=InnoDB DEFAULT CHARSET=utf8"
    :conditional? true}))
