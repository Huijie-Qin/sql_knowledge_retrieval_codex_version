---------------------------------------------------------------------------------------------------------------
-- 任务描述：电商分渠道付费品类明细表
-- 创建记录：xxx
-- 修改记录：xxx
---------------------------------------------------------------------------------------------------------------


-- CREATE TABLE IF NOT EXIST biads.ads_ba_adv_ecommerce_category_channel_paid_analysis_dm
-- (
--   source ,  STRING  COMMENT '广告/全渠道'
--   xxxx
--   xxxx
--   xxxx
--   xxxx
--   xxxx
-- ;


-- 广告
DROP TABLE IF EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_1_$date;
CREATE TABLE IF NOT EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_1_$date AS
SELECT
	did
	,event_type
	,creative_content_first_dev
	,creative_content_second_dev
	,creative_content_third_dev
	,creative_content_fourth_dev
	,COUNT(1) as cnt
	,SUM(dwagg_actual_spent) AS dwagg_actual_spent
FROM biads.ads_ba_adv_ecommerce_category_channel_paid_analysis_dm
WHERE pt_d='$date'
AND event_type IN ('exposure', 'click', 'paid')
AND creative_content_first_id_dev='02'
GROUP BY
	did
	,event_type
        ,creative_content_first_dev
        ,creative_content_second_dev
        ,creative_content_third_dev
        ,creative_content_fourth_dev
;


DROP TABLE IF EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_2_$date;
CREATE TABLE IF NOT EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_2_$date AS
SELECT
 	'一级'	AS category_level
	,creative_content_first_dev	AS  category_name
	,event_type
	,SUM(cnt)	AS cnt
	,SUM(dwagg_actual_spent)	AS dwagg_actual_spent
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_1_$date
GROUP BY creative_content_first_dev,event_type
UNION ALL
SELECT
        '二级'  AS category_level
        ,creative_content_second_dev     AS  category_name
        ,event_type
        ,SUM(cnt)       AS cnt
        ,SUM(dwagg_actual_spent)        AS dwagg_actual_spent
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_1_$date
GROUP BY creative_content_second_dev,event_type
UNION
SELECT
        '三级'  AS category_level
        ,creative_content_third_dev     AS  category_name
        ,event_type
        ,SUM(cnt)       AS cnt
        ,SUM(dwagg_actual_spent)        AS dwagg_actual_spent
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_1_$date
GROUP BY creative_content_third_dev,event_type
UNION
SELECT
        '四级'  AS category_level
        ,creative_content_fourth_dev     AS  category_name
        ,event_type
        ,SUM(cnt)       AS cnt     
        ,SUM(dwagg_actual_spent)        AS dwagg_actual_spent
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_1_$date
GROUP BY creative_content_fourth_dev,event_type
;



-- 全渠道
DROP TABLE IF EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_3_$date;
CREATE TABLE IF NOT EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_3_$date AS
SELECT
	'电商快消' AS first_category_name
	,second_category_name
	,third_category_name
	,forth_category_name
	,SUM(cnt) AS cnt
FROM
(
	SELECT
		usid
		,industry_second_class	AS normalized_tag2_id
		,industry_third_class  AS normalized_tag3_id
		,industry_forth_class  AS normalized_tag4_id
		,COUNT(1)	AS cnt
		FROM	biads.ads_usidpersona_sample_all_channel_ecommerce_app_dm
		WHERE pt_d = '$last_four_day'
		AND channel = '全渠道'	
		AND event_type = 'paid'
		AND industry_first_class = '02'
		GROUP BY usid
			,industry_second_class
			,industry_third_class
			,industry_forth_class
) t1
LEFT JOIN
(
	SELECT
		materials_label_id AS category_id
		,materials_label_zh AS second_category_name
	FROM pps_dspperfm.dim_ad_materials_label_info_mapping_ds
	WHERE pt_d='$last_date'
	AND materials_label_id like '02%'
	AND materials_label_type = '二级标签'
)t2
ON t1.normalized_tag2_id=t2.category_id
LEFT JOIN
(
	SELECT
		materials_label_id   AS category_id
                ,materials_label_zh AS third_category_name
	FROM pps_dspperfm.dim_ad_materials_label_info_mapping_ds
	WHERE pt_d='$last_date'
	AND materials_label_id like '02%'
        AND materials_label_type = '三级标签'

)t3
ON t1.normalized_tag3_id=t3.category_id
LEFT JOIN
(
	 SELECT
                materials_label_id   AS category_id
                ,materials_label_zh AS forth_category_name
        FROM pps_dspperfm.dim_ad_materials_label_info_mapping_ds
        WHERE pt_d='$last_date'
        AND materials_label_id like '02%'
        AND materials_label_type = '四级标签'

)t4
ON t1.normalized_tag4_id=t4.category_id
GROUP BY
	second_category_name
	,third_category_name
	,forth_category_name
;



DROP TABLE IF EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_4_$date;
CREATE TABLE IF NOT EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_4_$date AS
SELECT
	'一级'	AS category_level
	,first_category_name	AS category_name
	,SUM(cnt) AS cnt
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_3_$date
GROUP BY first_category_name
UNION ALL
SELECT 
	'二级'  AS category_level
        ,second_category_name    AS category_name
        ,SUM(cnt) AS cnt
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_3_$date
GROUP BY second_category_name
UNION ALL
SELECT 
        '三级'  AS category_level
        ,third_category_name    AS category_name
        ,SUM(cnt) AS cnt
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_3_$date
GROUP BY third_category_name
UNION ALL
SELECT 
        '四级'  AS category_level
        ,forth_category_name    AS category_name
        ,SUM(cnt) AS cnt
FROM temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_3_$date
GROUP BY forth_category_name
;


-- 全渠道最近七天二级品类排名
DROP TABLE IF EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_5_$date;
CREATE TABLE IF NOT EXISTS temp.tmp_ads_ba_adv_ecommerce_category_channel_paid_analysis_dm_5_$date AS
SELECT
	normalized_tag2_id
	,ROW_NUMBER() OVER(ORDER BY tag2_cnt DESC) AS tag2_rn
FROM
(
	SELECT
		industry_second_class AS normalized_tag2_id
		,COUNT(1) AS tag2_cnt
		FROM biads.ads_usidpersona_sample_all_channel_ecommerce_app_dm
		WHERE pt_d=bicoredata.dateformat(DATE_SUB('$date_ep',4))
		AND pt_d=bicoredata.dateformat(DATE_SUB('$date_ep',10))
		AND channel='全渠道'
		AND event_type='paid'
		AND industry_first_class = '02'
		GROUP BY industry_second_class
)t2
;























