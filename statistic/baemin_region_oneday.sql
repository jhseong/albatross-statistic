/*****************************************************
* 2-3. 배민 도/광역별 일일 통계 분석(Pie)
* ----------------------------------------------------
* - 리스팅만(BaeminSearchLog 제외)
******************************************************/
WITH 
  region1 AS (
    select c.det_cd AS region_large_cd, c.cd_nm AS region_large_nm
      from sbsvc.comm_cd c
     where c.main_cd = 'B0006'  -- 지역대분류코드
       AND c.use_yn = 'Y'       -- 사용유무 
       AND c.ref_9 = 'Y'        -- 노출유무
  ),
  region3 AS (
    SELECT c.ref_1 AS region_large_cd, c.det_cd AS region_small_cd
      FROM sbsvc.comm_cd c
     WHERE c.main_cd = 'B0008'  -- 지역소분류코드
       AND c.use_yn = 'Y'       -- 사용유무
  ),
  userlog AS (
    SELECT   log.log_dt
           , log.logtype
           , log.request
           , substr(regexp_extract(log.request, '(subRegionId4User)(\W+)([0-9]+)'), length('subRegionId4User') + 3) AS subRegionId4User
        --   , substr(log.request, position('subRegionId4User' IN log.request) + 18, 8) AS subRegionId4User
      FROM sblog.ad_listing_api_log log
     WHERE log.env = 'RELEASE'
       AND log.log_dt = date_format(current_date, '%Y-%m-%d')
       AND log.servicetype = 'BAEMIN'
       AND log.logtype != 'BaeminSearchLog'
  )
  SELECT r1.region_large_nm, COUNT(r1.region_large_nm) AS "일일요청건수"
    FROM region1 r1, region3 r3, userlog l
   WHERE r3.region_small_cd = l.subRegionId4User
     AND r3.region_large_cd = r1.region_large_cd
 GROUP BY r1.region_large_nm
 ORDER BY COUNT(r1.region_large_nm) DESC
