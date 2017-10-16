/******************************************************************************
* 배민/배라 운영 이슈 해결건
* ----------------------------------------------------------------------------
* 리스팅만 포함됨.
* 특정 업주 or 업소/ 지역/ 카테고리/ 시간대 지정해야 함.
******************************************************************************/

WITH user_activity AS (
    SELECT log.log_dt
         , log.log_date
         , log.log_date + interval '9' hour AS kst_log_date
         , log.logtype
         , substr(regexp_extract(log.request, '(subRegionId4User)(\W+)([0-9]+)'), length('subRegionId4User') + 3) AS region_small_cd
         , substr(regexp_extract(log.request, '(categoryId)(\W+)([0-9]+)'), length('categoryId') + 3) AS category_id
         , substr(regexp_extract(log.request, '(page)(\W+)({)(.+)(})'), length('page') + 3) AS page 
         , log.request
         , log.resultshops
      FROM sblog.ad_listing_api_log log
     WHERE log.env = 'RELEASE'
       AND log.log_dt >= SUBSTR(TRIM('${*1_조회시작일자_시분초*}'), 1, 10)
       AND log.log_dt <= SUBSTR(TRIM('${*1_조회종료일자_시분초*}'), 1, 10)
       AND log.log_date >= timestamp '${*1_조회시작일자_시분초*}' - interval '9' hour
       AND log.log_date <= timestamp '${*1_조회종료일자_시분초*}' - interval '9' hour
       AND log.logtype IN ('BaeminListingLog', 'BaeminFranchiseListingLog', 'RidersListingLog')
) 
, shop_info AS (
      SELECT shop_no, shop_nm, rgn3_cd AS region_small_cd, rgn1_cd AS region_large_cd, ct_cd AS category_cd
        FROM sbsvc.shop
       WHERE REGEXP_LIKE(shop_owner_no, TRIM('${*2_1_업주번호}')) = true
         AND REGEXP_LIKE(shop_no, TRIM('${*2_2_업소번호}')) = true
         AND REGEXP_LIKE(shop_nm, TRIM('${*2_2_업소명}')) = true
         AND use_yn = 'Y'
         AND block_yn = 'N'
) 
, region_info AS (   
    SELECT r1.det_cd AS region_large_cd, r1.cd_nm AS region_large_nm, r3.det_cd AS region_small_cd, r3.cd_nm AS region_small_nm
      FROM sbsvc.comm_cd r1, sbsvc.comm_cd r3
     WHERE r1.main_cd = 'B0006'  -- 지역대분류코드
       AND r1.use_yn = 'Y'       -- 대분류사용유무 
       AND r1.ref_9 = 'Y'        -- 대분류노출유무
       AND r3.main_cd = 'B0008'  -- 지역소분류코드
       AND r3.use_yn = 'Y'       -- 소분류사용유무
       AND r1.det_cd = r3.ref_1
       AND REGEXP_LIKE(r3.det_cd, TRIM('${3_동코드}')) = true
       AND REGEXP_LIKE(r3.cd_nm, TRIM('${3_동명}')) = true
)
, category_info AS (
    SELECT  c.det_cd AS category_cd, c.cd_nm AS category_nm
      FROM  sbsvc.comm_cd c
     WHERE  c.main_cd = 'B0013' -- 카테고리
       AND  c.use_yn = 'Y'
       AND  REGEXP_LIKE(c.det_cd, TRIM('${4_카테고리ID}')) = true
       AND  REGEXP_LIKE(c.cd_nm, TRIM('${4_카테고리명}')) = true
)
SELECT    shop_nm AS "업소명", shop_no AS "업소번호"
        , MIN(find_yn) AS "노출유무", MAX(find_rank) AS "노출순위"
        , region_large_nm AS "대지역명", region_large_cd AS "대지역코드", region_small_nm AS "소지역명", region_small_cd AS "소지역코드"
        , kst_log_date AS "요청시간"      
        , category_nm AS "카테고리명", category_cd AS "카테고리ID"
        , page AS "요청페이지"
        , resultshops AS "노출업소"
  FROM (
        SELECT   CASE result_shopId WHEN shop_no THEN 'O' ELSE 'X' END AS find_yn
               , CASE result_shopId WHEN shop_no THEN result_rank ELSE -99 END AS find_rank
               , shop_nm, shop_no
               , region_large_nm, region_large_cd, region_small_nm, region_small_cd
               , category_nm, category_cd
               , logtype
               , kst_log_date
               , page
               , result_shopId, result_rank
               , resultshops
          FROM (
              SELECT s.shop_nm, s.shop_no
                   , r.region_large_nm, r.region_large_cd, r.region_small_nm, r.region_small_cd
                   , c.category_nm, c.category_cd
                   , ua.logtype
                   , ua.log_dt, ua.log_date, ua.kst_log_date
                   , ua.request, ua.resultshops
                   , ua.page
                FROM shop_info s, region_info r, user_activity ua, category_info c
               WHERE s.region_small_cd = r.region_small_cd
                 AND s.region_large_cd = r.region_large_cd
                 AND s.category_cd = c.category_cd
                 AND r.region_small_cd = ua.region_small_cd
                 AND s.category_cd = ua.category_id
             )
        CROSS JOIN UNNEST(resultshops) WITH ORDINALITY AS t (result_shopId, result_rank)
      )
GROUP BY shop_nm, shop_no, region_large_nm, region_large_cd, region_small_nm, region_small_cd, category_nm, category_cd, logtype, kst_log_date, page, resultshops
ORDER BY shop_nm, shop_no, region_large_nm, region_large_cd, region_small_nm, region_small_cd, kst_log_date, category_nm, category_cd
