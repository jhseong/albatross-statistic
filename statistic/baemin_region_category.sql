/******************************************************************
* 2-5. 배민: 이 광역/도는 어느 카테고리를 자주 이용하는지? 추이 분석
* -----------------------------------------------------------------
* - 리스팅만(BaeminSearchLog 제외)
*******************************************************************/
WITH region_info AS (   
    SELECT r1.det_cd AS region_large_cd, r1.cd_nm AS region_large_nm, r3.det_cd AS region_small_cd, r3.cd_nm AS region_small_nm
      FROM sbsvc.comm_cd r1, sbsvc.comm_cd r3
     WHERE r1.main_cd = 'B0006'  -- 지역대분류코드
       AND r1.use_yn = 'Y'       -- 대분류사용유무 
       AND r1.ref_9 = 'Y'        -- 대분류노출유무
       AND r3.main_cd = 'B0008'  -- 지역소분류코드
       AND r3.use_yn = 'Y'       -- 소분류사용유무
       AND r1.det_cd = r3.ref_1
       AND (r1.cd_nm = '${도/광역명}' OR r1.det_cd = '${도/광역코드}')
)
, category_info AS (
    SELECT  c.det_cd AS category_id, c.cd_nm AS category_nm
      FROM  sbsvc.comm_cd c
     WHERE  c.main_cd = 'B0013' -- 카테고리
       AND  (c.use_yn = 'Y' OR c.ref_13 = 'Y') -- 사용유무 , 카테고리 사용유무.
)
, user_activity AS (
    SELECT log.log_dt
         , substr(regexp_extract(log.request, '(subRegionId4User)(\W+)([0-9]+)'), length('subRegionId4User') + 3) AS region_small_cd
         , substr(regexp_extract(log.request, '(categoryId)(\W+)([0-9]+)'), length('categoryId') + 3) AS category_id
      FROM sblog.ad_listing_api_log log
     WHERE log.env = 'RELEASE'
       AND log.log_dt >= date_format(date_add('day', -7, current_date), '%Y-%m-%d')
       AND log.log_dt <= date_format(current_date, '%Y-%m-%d')
       AND log.servicetype = 'BAEMIN'
       AND log.logtype != 'BaeminSearchLog'
)
  SELECT ua.log_dt, c.category_nm, COUNT(c.category_nm)
    FROM region_info r, user_activity ua, category_info c
   WHERE r.region_small_cd = ua.region_small_cd
     AND ua.category_id = c.category_id
GROUP BY ua.log_dt, c.category_nm
  HAVING COUNT(c.category_nm) > 0
ORDER BY ua.log_dt, COUNT(c.category_nm) DESC

