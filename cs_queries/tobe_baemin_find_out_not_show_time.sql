WITH user_activity AS (
    SELECT *
      FROM (
            SELECT   log_dt
                   , log_date
                   , kst_log_date
                   , region_small_cd
                   , category_id
                   , page
                   , ranking
                   , CAST(json_extract(result_shop_info, '$.shopNumber') AS VARCHAR) AS result_shop_id
                   , request
                   , result_shop_info
                   , resultshops
              FROM (
                  SELECT log_dt
                       , log_date
                       , log_date + interval '9' hour AS kst_log_date
                       , request
                       , resultshops
                       , CAST(json_extract(request, '$.subRegionId4User') AS VARCHAR) AS region_small_cd
                       , CAST(json_extract(request, '$.categoryId') AS VARCHAR) AS category_id
                       , json_extract(request, '$.page') AS page
                    FROM  sblog.ad_listing_api_log log
                   WHERE 1=1
                     AND log.env = '${*0_운영_테스트서버=RELEASE,RELEASE|TEST}'
                     AND log.log_dt >= SUBSTR(TRIM('${*1_조회시작일자_시분초*}'), 1, 10)
                     AND log.log_dt <= SUBSTR(TRIM('${*1_조회종료일자_시분초*}'), 1, 10)
                     AND log.log_date >= timestamp '${*1_조회시작일자_시분초*}' - interval '9' hour
                     AND log.log_date <= timestamp '${*1_조회종료일자_시분초*}' - interval '9' hour
                     AND log.log_hour >= CAST(SUBSTR(TRIM('${*1_조회시작일자_시분초*}'), 12, 2) AS INTEGER)
                     AND log.log_hour <= CAST(SUBSTR(TRIM('${*1_조회종료일자_시분초*}'), 12, 2) AS INTEGER)
                     AND log.logtype = ('${5_로그타입=BaeminListingLog,BaeminListingLog|BaeminFranchiseListingLog|RidersListingLog}')
                   )
            CROSS JOIN UNNEST(resultshops) WITH ORDINALITY AS t (result_shop_info, ranking)
        )
      WHERE 1=1
        AND IF('${4_카테고리ID}'='', 1=1, category_id = TRIM('${4_카테고리ID}'))
)
, shop_info AS (
      SELECT  s.shop_no, s.shop_nm
            , s.rgn3_cd AS region_small_cd, s.rgn2_cd AS region_middle_cd, s.rgn1_cd AS region_large_cd
            , ct_cd AS category_cd
            , c.cd_nm AS category_nm
        FROM sbsvc.shop s, sbsvc.comm_cd c
       WHERE 1=1
         AND REGEXP_LIKE(s.shop_no, TRIM('${*2_업소번호}')) = true
         AND s.use_yn = 'Y'
         AND s.block_yn = 'N'
         AND s.ct_cd = c.det_cd  -- 카테고리ID
         AND c.main_cd = 'B0013' -- 카테고리
         AND c.use_yn = 'Y'
         AND IF('${4_카테고리ID}'='', 1=1, c.det_cd = TRIM('${4_카테고리ID}'))
         AND IF('${4_카테고리명}'='', 1=1, c.cd_nm = TRIM('${4_카테고리명}'))
)
, region_info AS (   
    SELECT r1.det_cd AS region_large_cd, r1.cd_nm AS region_large_nm
         , r2.det_cd AS region_middle_cd, r2.cd_nm AS region_middle_nm
         , r3.det_cd AS region_small_cd, r3.cd_nm AS region_small_nm
      FROM sbsvc.comm_cd r1, sbsvc.comm_cd r2, sbsvc.comm_cd r3
     WHERE 1=1
       AND r1.main_cd = 'B0006'  -- 지역대분류코드
       AND r1.use_yn = 'Y'       -- 대분류사용유무 
       AND r1.ref_9 = 'Y'        -- 대분류노출유무
       AND r2.main_cd = 'B0007'  -- 지역중분류코드
       AND r2.use_yn = 'Y'       -- 중분류노출유무
       AND r3.main_cd = 'B0008'  -- 지역소분류코드
       AND r3.use_yn = 'Y'       -- 소분류사용유무
       AND r3.ref_1 = r1.det_cd
       AND r3.ref_2 = r2.det_cd
       AND r2.ref_1 = r1.det_cd
       AND IF('${3_동코드}'='', 1=1, r3.det_cd = TRIM('${3_동코드}'))
       AND IF('${3_동명}'='', 1=1, r3.cd_nm = TRIM('${3_동명}'))
)
SELECT   kst_log_date as "요청시간"
    --   , date_trunc('minute', kst_log_date) as "요청시간분"
       , shop_no as "업소번호"
       , shop_nm as "업소명"
       , MIN(find_yn) as "노출여부"
    --   , MAX(ranking) as "노출순위"
       , category_id as "카테고리ID"
       , category_nm as "카테고리명"
    --   , region_large_cd
       , region_large_nm as "대지역명"
    --   , region_middle_cd
       , region_middle_nm as "중지역명"
       , region_small_cd as "소지역코드"
       , region_small_nm as "소지역명"
       , page as "요청페이지"
    --   , request
       , resultshops as "노출업소전체"
  FROM (
        SELECT   ua.log_dt
            --   , ua.log_date
               , ua.kst_log_date
            --   , 'ua-->'
               , ua.region_small_cd
               , ua.category_id
            --   , 's-->'
            --   , s.region_small_cd       
            --   , s.category_cd       
               , s.category_nm       
            --   , 'r-->'
            --   , r.region_small_cd
               , r.region_small_nm
            --   , r.region_middle_cd 
               , r.region_middle_cd
               , r.region_middle_nm              
               , r.region_large_cd
               , r.region_large_nm
               , s.shop_no
               , s.shop_nm
               , ua.page
               , ua.result_shop_id
               , ua.request
               , IF(ua.result_shop_id = s.shop_no, 'O', 'X') AS find_yn
               , IF(ua.result_shop_id = s.shop_no, ua.ranking, -99) AS ranking
               , ua.result_shop_info
               , IF('${6_노출업소전체출력=Y,Y|N}' = 'Y', ua.resultshops, null) AS resultshops
          FROM  user_activity ua
              , shop_info s
              , region_info r
         WHERE 1=1
        --   AND ua.result_shop_id = s.shop_no
          AND ua.category_id = s.category_cd
          AND s.region_middle_cd = r.region_middle_cd
          AND r.region_small_cd = ua.region_small_cd
       )
  WHERE ranking = -99 
GROUP BY shop_no, shop_nm
       , category_id, category_nm
       , region_large_cd, region_large_nm, region_middle_cd, region_middle_nm, region_small_cd, region_small_nm
       , kst_log_date
       , page
    --   , request
       , resultshops
ORDER BY kst_log_date
       , category_id, category_nm
       , region_large_cd, region_large_nm, region_middle_cd, region_middle_nm, region_small_cd, region_small_nm
       , CAST(json_extract(page, '$.from') AS INTEGER)
