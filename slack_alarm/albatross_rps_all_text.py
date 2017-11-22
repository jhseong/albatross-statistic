%python


from pyhive import presto
from datetime import datetime, timedelta
import json
import requests
import numbers
import traceback

# To solve Pesto Encoding Problem with Korean
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

PRESTO_DEFAULT_HOST = '172.31.1.2'
PRESTO_DEFAULT_PORT = '12303'
PRESTO_DEFAULT_USER = 'zeppelin'

# albatross-info channel
REAL_SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'
TEST_SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'
SLACK_WEBHOOK_URL = z.select("Slack_Channel",[(TEST_SLACK_WEBHOOK_URL, "test_channel"),(REAL_SLACK_WEBHOOK_URL, "albatross-info")], REAL_SLACK_WEBHOOK_URL)

class WoowahanPresto(object):
    def __init__(self, host=PRESTO_DEFAULT_HOST, port=PRESTO_DEFAULT_PORT, user=PRESTO_DEFAULT_USER):
        self.host = host
        self.port = port
        self.user = user

    def connect(self):
        self.cursor = presto.connect(self.host, self.port).cursor()

    def fetchone(self, query):
        self.__execute(query)
        return self.cursor.fetchone()

    def fetchall(self, query):
        self.__execute(query)
        return self.cursor.fetchall()

    def string_date_from_today(self, interval):
        return self.fetchone("SELECT date_format(current_date + interval '{0}' day, '%Y-%m-%d')".format(interval))
        
    def today(self):
        return self.string_date_from_today(0)

    def close(self):
        if self.cursor:
            self.cursor.close()
        else:
            raise Error('There is no connection to close.')

    def __execute(self, query):
        self.cursor.execute(query)


class QueryUtils:
    def __init__(self, env):
        self.env = env

    def albatross_daily_app_usage(self, date):
        return        (" SELECT servicetype || ' ' || logtype AS logtype, request_cnt "
                       "   FROM ( "
                       "        SELECT servicetype, logtype, COUNT(logtype) AS request_cnt  "
                       "           FROM (   "
                       "             SELECT CASE WHEN regexp_like(log.logtype, '.ListingLog$') = TRUE THEN 'Listing' "
                       "                         WHEN regexp_like(log.logtype, '.SearchLog$') = TRUE THEN 'Search' "
                       "                         WHEN regexp_like(log.logtype, '.CurationLog$') = TRUE THEN 'Curation' "
                       "                         WHEN regexp_like(log.logtype, '^ShopOpenStatusLog$') = TRUE THEN 'ShopOpen' "
                       "                         ELSE log.logtype   "
                       "                     END AS logtype "
                       "                   , CASE servicetype WHEN 'BAEMIN' THEN 'Baemin' "
                       "                                      WHEN 'RIDERS' THEN 'Riders' "
                       "                                      ELSE servicetype "
                       "                     END AS servicetype "
                       "               FROM sblog.ad_listing_api_log log   "
                       "              WHERE log.log_dt = '{date}' "
                       "                AND log.log_hour BETWEEN 0 AND 23   "
                       "                AND log.env = '{env}'   "
                       "                AND log.logtype != 'RidersShopInquiryLog'   "
                       "            )   "
                       "      GROUP BY servicetype, logtype   "
                       "      ORDER BY servicetype, logtype   "
                       "        ) "  ).format(date=date, env=self.env)
                
    def albatross_daily_max_rps(self, date):
        return (" " 
            " WITH user_activities AS ( "
            "     SELECT  CASE log.servicetype WHEN 'BAEMIN' THEN 'Baemin' "
            "                                  WHEN 'RIDERS' THEN 'Riders' "
            "                                  ELSE log.servicetype "
            "             END AS servicetype "
            "           , log.logtype, date_format(log.log_date, '%Y-%m-%d %H:%i:%s') AS request_seconds "
            "           , COUNT(log.log_date) AS request_cnt "
            "       FROM sblog.ad_listing_api_log log "
            "      WHERE log.log_dt = '{date}' "
            "        AND log_hour BETWEEN 0 AND 23 "            
            "        AND log.env = '{env}' "
            "   GROUP BY log.servicetype, log.logtype, date_format(log.log_date, '%Y-%m-%d %H:%i:%s') "
            " ) "
            " SELECT * "
            "   FROM ( "
            "     SELECT 'Total' AS title "
            "           , MAX(tot_request_cnt) AS max_request_cnt "
            "       FROM ( "
            "           SELECT request_seconds, SUM(request_cnt) AS tot_request_cnt "
            "             FROM user_activities ua "
            "         GROUP BY request_seconds "
            "           ) "
            "     UNION "
            "     SELECT  servicetype AS title "
            "           , MAX(tot_request_cnt) AS max_request_cnt "
            "       FROM ( "
            "          SELECT request_seconds, ua.servicetype, SUM(request_cnt) AS tot_request_cnt "
            "             FROM user_activities ua "
            "         GROUP BY request_seconds, ua.servicetype "
            "       ) "
            "     GROUP BY servicetype "
            "     UNION "
            "     SELECT  concat(servicetype, ' ', logtype) AS title "
            "           , MAX(tot_request_cnt) AS max_request_cnt "
            "       FROM ( "
             "              SELECT request_seconds  "                                                             
             "                   , ua.servicetype   "
             "                   , CASE WHEN regexp_like(ua.logtype, '.ListingLog$') = true THEN 'Listing'  "
             "                          WHEN regexp_like(ua.logtype, '.SearchLog$') = true THEN 'Search'  "
             "                          WHEN regexp_like(ua.logtype, '.CurationLog$') = true THEN 'Curation' "
             "                          WHEN regexp_like(ua.logtype, '^ShopOpenStatusLog$') = TRUE THEN 'ShopOpen' "
             "                          ELSE ua.logtype  "
             "                      END AS logtype  "
             "                   , SUM(request_cnt) AS tot_request_cnt  "
             "                 FROM user_activities ua  "
             "             GROUP BY request_seconds "
             "                    , ua.servicetype  "
             "                    , CASE WHEN regexp_like(ua.logtype, '.ListingLog$') = true THEN 'Listing'  "
             "                          WHEN regexp_like(ua.logtype, '.SearchLog$') = true THEN 'Search'  "
             "                          WHEN regexp_like(ua.logtype, '.CurationLog$') = true THEN 'Curation' "
             "                          WHEN regexp_like(ua.logtype, '^ShopOpenStatusLog$') = TRUE THEN 'ShopOpen' "
             "                          ELSE ua.logtype  "
             "                      END "
            "       ) "
            "     GROUP BY concat(servicetype, ' ', logtype) "
            " ) "
            " ORDER BY title ").format(date=date, env=self.env)

    def albatross_daily_max_rpm(self, date):
        return (" " 
                " WITH user_activities AS ( "
                "     SELECT  CASE log.servicetype WHEN 'BAEMIN' THEN 'Baemin' "
                "                                  WHEN 'RIDERS' THEN 'Riders' "
                "                                  ELSE '' "
                "             END AS servicetype "
                "           , log.logtype, date_format(log.log_date, '%Y-%m-%d %H:%i') AS request_per_minute "
                "           , COUNT(log.log_date) AS request_cnt "
                "       FROM sblog.ad_listing_api_log log "
                "      WHERE log.log_dt = '{date}' "
                "        AND log.log_hour BETWEEN 0 AND 23 "                
                "        AND log.env = '{env}' "
                "   GROUP BY log.servicetype, log.logtype, date_format(log.log_date, '%Y-%m-%d %H:%i') "
                " ) "
                " SELECT * "
                "   FROM ( "
                "     SELECT 'Total' AS title "
                "           , MAX(tot_request_cnt) AS max_request_cnt "
                "       FROM ( "
                "           SELECT request_per_minute, SUM(request_cnt) AS tot_request_cnt "
                "             FROM user_activities ua "
                "         GROUP BY request_per_minute "
                "           ) "
                "     UNION "
                "     SELECT  servicetype AS title "
                "           , MAX(tot_request_cnt) AS max_request_cnt "
                "       FROM ( "
                "          SELECT request_per_minute, ua.servicetype, SUM(request_cnt) AS tot_request_cnt "
                "             FROM user_activities ua "
                "         GROUP BY request_per_minute, ua.servicetype "
                "       ) "
                "     GROUP BY servicetype "
                "     UNION "
                "     SELECT  concat(servicetype, ' ', logtype) AS title "
                "           , MAX(tot_request_cnt) AS max_request_cnt "
                "       FROM ( "
                 "              SELECT request_per_minute  "                                                             
                 "                   , ua.servicetype   "
                 "                   , CASE WHEN regexp_like(ua.logtype, '.ListingLog$') = true THEN 'Listing'  "
                 "                          WHEN regexp_like(ua.logtype, '.SearchLog$') = true THEN 'Search'  "
                 "                          WHEN regexp_like(ua.logtype, '.CurationLog$') = true THEN 'Curation' "
                 "                          WHEN regexp_like(ua.logtype, '^ShopOpenStatusLog$') = TRUE THEN 'ShopOpen' "
                 "                          ELSE ua.logtype  "
                 "                      END AS logtype  "
                 "                   , SUM(request_cnt) AS tot_request_cnt  "
                 "                 FROM user_activities ua  "
                 "             GROUP BY request_per_minute "
                 "                    , ua.servicetype  "
                 "                    , CASE WHEN regexp_like(ua.logtype, '.ListingLog$') = true THEN 'Listing'  "
                 "                          WHEN regexp_like(ua.logtype, '.SearchLog$') = true THEN 'Search'  "
                 "                          WHEN regexp_like(ua.logtype, '.CurationLog$') = true THEN 'Curation' "
                 "                          WHEN regexp_like(ua.logtype, '^ShopOpenStatusLog$') = TRUE THEN 'ShopOpen' "
                 "                          ELSE ua.logtype  "
                 "                      END "
                "       ) "
                "     GROUP BY concat(servicetype, ' ', logtype) "
                " ) "
                " ORDER BY title ").format(date=date, env=self.env)


class Chat(object):
    def  __init__(self, webhook_url=SLACK_WEBHOOK_URL):
        self.webhook_url = webhook_url

    # send data to slack
    def post_slack(self, text=None, attachments=None):
        requests.packages.urllib3.disable_warnings()

        slack_data = json.dumps({"text": text, "attachments": attachments})
        response = requests.post(self.webhook_url, data=slack_data)
        if response.status_code != 200:
            raise ValueError(
                'Request to slack returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
            )

        return response


class SlackMessageUtils:
    pass

    def convert_list_to_linefeed_string(self, datas=[], index=0):
        results = []
        if isinstance(datas, list):
            for data in datas:
                value = data[index]
                if isinstance(value, numbers.Number):
                    value = "{:,d}".format(value)
                results.append(value)

        return "\n".join(results)

    def make_text(self, array_texts):
        text = []
        for texts in array_texts:
            if texts[0] is not None:
                text.append("\n" + texts[0])
            for value in texts[1]:
                text.append(" - " + str(value[0]) + ": " + str("{:,d}".format(value[1])))
        return "\n".join(text)
        
    def make_attachments(self, pretext=None, title=None, text=None, fields=None):
        attachments = [{}]
        attachments[0]['pretext'] = pretext
        attachments[0]['color'] = '#36a64f'
        attachments[0]['title'] = title
        attachments[0]['text'] = text
        attachments[0]["fields"] = [fields]
        return attachments

    def __string_date_format(self, date, dateformat = "%Y-%m-%d"):
        return datetime.strftime(date, dateformat)


class LoggerUtils:
    pass

    def ex_message_traceback(self, ex, ex_traceback=None):
        if ex_traceback is None:
            ex_traceback = ex.__traceback__
        tb_lines = [line.rstrip('\n') for line in
                    traceback.format_exception(ex.__class__, ex, ex_traceback)]
        return tb_lines



if __name__ == '__main__':

    chat = Chat()
    woowahanPresto = WoowahanPresto()
    
    env = z.select("Env", [("RELEASE", "RELEASE"), ("TEST", "TEST")], "RELEASE")
    queryUtils = QueryUtils(env)
    
    slackMessageUtils = SlackMessageUtils()

    try:
        print(env)        
        # 1. Connect
        woowahanPresto.connect()
    
        # 2. Execute SQL with Presto
        # 2-1. select today
        date_interval = z.input("Interval From Today", "0")
        presto_today = woowahanPresto.string_date_from_today(date_interval)
        inquery_date = ''.join(presto_today)
        print(inquery_date)
    
        # # 2-2-1. execute app usage query
        app_usage_query = queryUtils.albatross_daily_app_usage(inquery_date)
        presto_app_usage_query = woowahanPresto.fetchall(app_usage_query)
        print(presto_app_usage_query)
        
        # 2-2-2. execute rps query
        rps_query = queryUtils.albatross_daily_max_rps(inquery_date)
        presto_rps_results = woowahanPresto.fetchall(rps_query)
        print(presto_rps_results)
        
        # 2-2-3. execute rpm query
        rpm_query = queryUtils.albatross_daily_max_rpm(inquery_date)
        presto_rpm_results = woowahanPresto.fetchall(rpm_query)
        print(presto_rpm_results)
        
        # 3. close connection
        woowahanPresto.close()
    
        # 4. Make message for Slack
        array_texts = []
        array_texts.append(["[App Request]", presto_app_usage_query])
        array_texts.append(["[Max(RPS)]", presto_rps_results])
        array_texts.append(["[Max(RPM)]", presto_rpm_results])
        message = slackMessageUtils.make_text(array_texts)
        print(message)
    
        attachments = slackMessageUtils.make_attachments(pretext=None, title=None, text=message)
    
        # 5. Send message to Slack
        chat.post_slack(text="*`배민/신배라 일 통계 정상: {0}`*".format(inquery_date), attachments=attachments)
        print("\nattachments = {%s}" % attachments)

    except BaseException as e:
        # send error message to slack
        loggerUtils = LoggerUtils()
        _, _, ex_traceback = sys.exc_info()
        exception_message = "\n".join(loggerUtils.ex_message_traceback(e, ex_traceback))

        print(exception_message)

        attachments = slackMessageUtils.make_attachments(pretext=None, title=None, text=exception_message)
        chat.post_slack(text="*`배민/신배라 일 통계 실패: {0}`*".format(inquery_date), attachments=attachments)


