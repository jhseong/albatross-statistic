/******************************************************************
* 3-1. 배민: 카테고리별 이용 추이 분석
* -----------------------------------------------------------------
* - 리스팅만(BaeminSearchLog 제외)
*******************************************************************/
WITH category_info AS (
    SELECT  c.det_cd AS category_id, c.cd_nm AS category_nm
      FROM  sbsvc.comm_cd c
     WHERE  c.main_cd = 'B0013' -- 카테고리
       AND  (c.use_yn = 'Y' OR c.ref_13 = 'Y') -- 사용유무 , 카테고리 사용유무... <--- 프랜차이즈 나눠야 하는지 확인.'    
)
, user_activity AS (
    SELECT log.log_dt
         , substr(regexp_extract(log.request, '(categoryId)(\W+)([0-9]+)'), length('categoryId') + 3) AS category_id
      FROM sblog.ad_listing_api_log log
     WHERE log.env = 'RELEASE'
       AND log.log_dt >= date_format(date_add('day', -14, current_date), '%Y-%m-%d')
       AND log.log_dt <= date_format(current_date, '%Y-%m-%d')
       AND log.servicetype = 'BAEMIN'
       AND log.logtype != 'BaeminSearchLog'
)
  SELECT ua.log_dt, c.category_nm, COUNT(c.category_nm) AS "일이용건수"
    FROM user_activity ua, category_info c
   WHERE ua.category_id = c.category_id
GROUP BY ua.log_dt, c.category_nm
  HAVING COUNT(c.category_nm) > 0
ORDER BY ua.log_dt, COUNT(c.category_nm) DESC
