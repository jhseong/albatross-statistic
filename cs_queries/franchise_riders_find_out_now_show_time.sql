WITH user_activity AS (
    SELECT result_shop_info ,
          CAST(json_extract(result_shop_info, '$.shopNumber') AS VARCHAR) AS result_shop_id ,
          franchiseNumber,
          ranking ,
          log_dt ,
          log_date ,
          kst_log_date ,
          logtype,
          region_small_cd,
          category_id,
          page,
          resultshops
    FROM
      ( 
            SELECT log_dt ,
                   log_date ,
                   log_date + interval '9' hour AS kst_log_date ,
                   logtype ,
                   CAST(json_extract(request, '$.franchiseNumber') AS VARCHAR) AS franchiseNumber,
                   CAST(json_extract(request, '$.subRegionId4User') AS VARCHAR) AS region_small_cd,
                   CAST(json_extract(request, '$.categoryId') AS VARCHAR) AS category_id ,
                   json_extract(request, '$.page') AS page,               
                   request,
                   resultshops
           FROM sblog.ad_listing_api_log log
          WHERE 1=1
            AND log.env = '${*0_운영_테스트서버=RELEASE,RELEASE|TEST}'
            AND log.log_dt >= SUBSTR(TRIM('${*1_조회시작일자_시분초*}'), 1, 10)
            AND log.log_dt <= SUBSTR(TRIM('${*1_조회종료일자_시분초*}'), 1, 10)
            AND log.log_date >= timestamp '${*1_조회시작일자_시분초*}' - interval '9' hour
            AND log.log_date <= timestamp '${*1_조회종료일자_시분초*}' - interval '9' hour
            AND log.log_hour >= CAST(SUBSTR(TRIM('${*1_조회시작일자_시분초*}'), 12, 2) AS INTEGER)
            AND log.log_hour <= CAST(SUBSTR(TRIM('${*1_조회종료일자_시분초*}'), 12, 2) AS INTEGER)
            AND log.logtype = ('${4_로그타입=BaeminFranchiseListingLog,BaeminFranchiseListingLog|RidersListingLog}')
    ) a 
    CROSS JOIN UNNEST(a.resultshops) WITH ORDINALITY AS t (result_shop_info, ranking)
)
, shop_info AS (
    select  shop_no, shop_nm
          , rgn3_cd AS region_small_cd
          , rgn2_cd AS region_middle_cd
          , rgn1_cd AS region_large_cd
          , ct_cd AS category_cd
      from sbsvc.shop s
     where 1=1
       AND IF('${*2_1_업주번호}'='', 1=1, s.shop_owner_no = TRIM('${*2_1_업주번호}'))
       AND IF('${*2_2_업소번호}'='', 1=1, s.shop_no = TRIM('${*2_2_업소번호}'))
       AND s.use_yn = 'Y'
       AND s.block_yn = 'N'
) 
, region_info AS (   
    select r3.cd_nm as region_small_nm, r3.det_cd as region_small_cd
         , r2.det_cd as region_middle_cd, r2.cd_nm as region_middle_nm
         , r1.det_cd as region_large_cd, r1.cd_nm as region_large_nm
      from sbsvc.comm_cd r3
         , sbsvc.comm_cd r2
         , sbsvc.comm_cd r1
     where r3.main_cd = 'B0008'
      and r3.use_yn = 'Y'
      and r2.main_cd = 'B0007'
      and r2.use_yn = 'Y'
      and r2.det_cd = r3.ref_2
      and r1.main_cd = 'B0006'
      and r1.det_cd = r3.ref_1
      and r1.use_yn = 'Y'
      AND IF('${3_동코드}'='', 1=1, r3.det_cd = TRIM('${3_동코드}'))
      AND IF('${3_동명}'='', 1=1, r3.cd_nm = TRIM('${3_동명}'))
)
SELECT 
        shop_nm AS "업소명"
      , shop_no as "업소번호"
    --   , MAP(ARRAY[fr_nm], ARRAY[fr_no]) as "프렌차이즈"
    --   , fr_nm as "프렌차이즈명"
      , fr_no as "프렌차이즈번호"
    --   , shop_category_cd as "카테고리"
      , MAP(ARRAY['대', '중', '소'], ARRAY[shop_region_large_cd, shop_region_middle_cd, shop_region_smll_cd]) AS "업소지역"
    --   , shop_region_large_cd as "업소대지역"
    --   , shop_region_middle_cd  
    --   , shop_region_smll_cd
      , MIN(find_yn) AS "노출여부"
    --   , MAX(ranking) AS "순위"
      , kst_log_date AS "요청시간"
      , page AS "요청페이지"
      , MAP(ARRAY[region_large_nm, region_middle_nm, region_small_nm], ARRAY[region_large_cd, region_middle_cd, region_small_cd]) AS "사용자_위치"
    --   , region_large_nm, region_large_cd
    --   , region_middle_nm, region_middle_cd
    --   , region_small_nm, region_small_cd
      , MAX(result_shop_info) AS "노출업소상세정보"
      , resultshops AS "노출업소전체"
  FROM (
        SELECT  s.shop_nm
              , s.shop_no
              , ua.franchiseNumber as fr_no
              , s.region_large_cd as "shop_region_large_cd"
              , s.region_middle_cd as "shop_region_middle_cd"
              , s.region_small_cd as "shop_region_smll_cd"
              , s.category_cd as "shop_category_cd"      
            --   , ua.ranking
            --   , ua.result_shop_id
              , IF(s.shop_no = ua.result_shop_id, 'O', 'X') as find_yn
              , IF(s.shop_no = ua.result_shop_id, ua.ranking, -99) as ranking
              , ua.kst_log_date      
            --   , ua.region_small_cd as "request_region_small_cd"
              , r.region_large_nm -- request_region_small_cd와 동일
              , r.region_large_cd
              , r.region_middle_nm
              , r.region_middle_cd
              , r.region_small_nm
              , r.region_small_cd
              , ua.logtype
              , ua.page
              , IF(s.shop_no = ua.result_shop_id, result_shop_info, null) as result_shop_info
              , IF('${5_노출업소전체출력=N,Y|N}' = 'N', null, ua.resultshops) AS resultshops
          FROM user_activity ua
             , shop_info s
             , region_info r
         WHERE 1=1
          AND r.region_small_cd = ua.region_small_cd
     ) 
   WHERE ranking = -99
GROUP BY shop_nm
      , shop_no
      , fr_no
      , shop_category_cd
      , shop_region_large_cd
      , shop_region_middle_cd  
      , shop_region_smll_cd
      , region_large_nm, region_large_cd
      , region_middle_nm, region_middle_cd
      , region_small_nm, region_small_cd
      , kst_log_date
      , page
      , resultshops
ORDER BY kst_log_date
      , region_large_nm, region_large_cd
      , region_middle_nm, region_middle_cd
      , region_small_nm, region_small_cd
      , CAST(json_extract(page, '$.from') AS INTEGER)
