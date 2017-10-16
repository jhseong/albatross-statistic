/*****************************************************
* 2-1. 배민 도/광역별 추이 통계 분석
* ----------------------------------------------------
* - 리스팅만(BaeminSearchLog 제외)* - 리스팅만(BaeminSearchLog 제외)
******************************************************/
WITH 
  region AS (
    select  r1.det_cd AS region_large_cd, r1.cd_nm AS region_large_nm
          , r3.ref_1 AS region_large_cd, r3.det_cd AS region_small_cd
      from sbsvc.comm_cd r1, sbsvc.comm_cd r3
     where r1.main_cd = 'B0006'  -- 지역대분류코드
       AND r1.use_yn = 'Y'       -- 사용유무 
       AND r1.ref_9 = 'Y'        -- 노출유무
       AND r3.main_cd = 'B0008'  -- 지역소분류코드
       AND r3.use_yn = 'Y'       -- 사용유무
       AND r1.det_cd = r3.ref_1
  )
  , userlog AS (
    SELECT   log.log_dt
           , log.logtype
           , log.request
           , substr(regexp_extract(log.request, '(subRegionId4User)(\W+)([0-9]+)'), length('subRegionId4User') + 3) AS subRegionId4User
        --   , substr(log.request, position('subRegionId4User' IN log.request) + 18, 8) AS subRegionId4User
      FROM sblog.ad_listing_api_log log
     WHERE log.env = 'RELEASE'
   AND log.log_dt >= date_format(date_add('day', -7, current_date), '%Y-%m-%d')
   AND log.log_dt <= date_format(current_date, '%Y-%m-%d')
   AND log.servicetype = 'BAEMIN'
   AND log.logtype != 'BaeminSearchLog'
  )
  SELECT r.region_large_nm, l.log_dt, COUNT(log_dt) AS CNT
    FROM region r, userlog l
   WHERE r.region_small_cd = l.subRegionId4User
GROUP BY r.region_large_nm, l.log_dt
ORDER BY l.log_dt, COUNT(log_dt) DESC, r.region_large_nm
