/*****************************************************
* 1-1. 배민 logtype별 추이 통계 분석
******************************************************/
SELECT log.log_dt
     , CASE log.logtype 
            WHEN 'BaeminListingLog' THEN '배민리스팅'
            WHEN 'BaeminRankShopListingLog' THEN '맛집랭킹'
            WHEN 'BaeminSearchLog' THEN '배민검색'
            WHEN 'BaeminFranchiseListingLog' THEN '프랜차이즈'
            ELSE log.logtype
        END AS "요청 타입"
     , COUNT(log.logtype) AS "1일 요청량"
  FROM sblog.ad_listing_api_log log
 WHERE log.env = 'RELEASE'
   AND log.log_dt >= date_format(date_add('day', -14, current_date), '%Y-%m-%d')
   AND log.log_dt <= date_format(current_date, '%Y-%m-%d')
   AND log.servicetype = 'BAEMIN'
GROUP BY log.log_dt, log.logtype
ORDER BY log.log_dt, COUNT(log.logtype) DESC
