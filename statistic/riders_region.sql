/*****************************************************
* 2-2. 배라 서울특별시 구별 추이 통계 분석
* ----------------------------------------------------
* - 리스팅만(RidersListingLog)
* - 배라는 서울만 서비스하고 있기 때문에 구별로 분석
******************************************************/
WITH 
  region_middle AS (
    select c.det_cd AS region_middle_cd, c.cd_nm AS region_middle_nm
      from sbsvc.comm_cd c
     where c.main_cd = 'B0007'  -- 지역중분류코드
       AND c.use_yn = 'Y'       -- 사용유무
       AND c.det_cd != '99999'    -- 우형시 제외
  ),
  region_small AS (
    SELECT c.ref_2 AS region_middle_cd, c.det_cd AS region_small_cd
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
       AND log.log_dt >= date_format(date_add('day', -14, current_date), '%Y-%m-%d')     
       AND log.log_dt <= date_format(current_date, '%Y-%m-%d')
       AND log.servicetype = 'RIDERS'
       AND log.logtype = 'RidersListingLog'
  )
  SELECT log.log_dt, rm.region_middle_nm, COUNT(rm.region_middle_nm) AS "일일요청건수"
    FROM region_middle rm, region_small rs, userlog log
   WHERE rs.region_small_cd = log.subRegionId4User
     AND rs.region_middle_cd = rm.region_middle_cd
 GROUP BY log.log_dt, rm.region_middle_nm
 ORDER BY log.log_dt, COUNT(rm.region_middle_nm) DESC, rm.region_middle_nm
