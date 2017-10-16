/*****************************************************
* 1-2. 배라 logtype별 추이 통계 분석
* ----------------------------------------------------
* - RidersShopInquiryLog는 제외처리
******************************************************/
SELECT log.log_dt
     , CASE log.logtype
            WHEN 'RidersListingLog' THEN '배라리스팅'
            WHEN 'RidersSearchLog' THEN '배라검색'
            ELSE log.logtype
        END AS "요청 타입"
     , COUNT(log.logtype) AS "1일 요청량"
  FROM sblog.ad_listing_api_log log
 WHERE log.env = 'RELEASE'
   AND log.log_dt >= date_format(date_add('day', -14, current_date), '%Y-%m-%d')
   AND log.log_dt <= date_format(current_date, '%Y-%m-%d')
   AND log.servicetype = 'RIDERS'
   AND log.logtype != 'RidersShopInquiryLog'
GROUP BY log.log_dt, log.logtype
ORDER BY log.log_dt, COUNT(log.logtype) DESC
