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
TEST_SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'


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

    def close(self):
        if self.cursor:
            self.cursor.close()
        else:
            raise Error('There is no connection to close.')

    def __execute(self, query):
        self.cursor.execute(query)


class QueryUtils:
    pass

    def albatross_daily_statistic(self, date):
        return ("  SELECT logtype, COUNT(logtype) "
                "     FROM ( "
                "       SELECT CASE log.logtype "
                "                   WHEN 'BaeminListingLog' THEN 'Baemin Listing' "
                "                   WHEN 'BaeminRankShopListingLog' THEN 'Baemin Listing' "
                "                   WHEN 'BaeminFranchiseListingLog' THEN 'Baemin Listing' "
                "                   WHEN 'BaeminSearchLog' THEN 'Baemin Search' "
                "                   WHEN 'RidersListingLog' THEN 'Riders Listing' "
                "                   WHEN 'RidersSearchLog' THEN 'Riders Search' "
                "                   ELSE log.logtype "
                "               END AS logtype "
                "         FROM sblog.ad_listing_api_log log "
                "        WHERE log_dt = '{query_date}' "
                "          AND log.env = 'RELEASE' "
                "          AND log.logtype != 'RidersShopInquiryLog' "
                "      ) "
                " GROUP BY logtype "
                " ORDER BY logtype ").format(query_date=date)

    def albatross_daily_max_rps(self, date):
        return (" "
            " WITH user_activities AS ( "
            "     SELECT  CASE log.servicetype WHEN 'BAEMIN' THEN 'Baemin' "
            "                                  WHEN 'RIDERS' THEN 'Riders' "
            "                                  ELSE '' "
            "             END AS servicetype "
            "           , log.logtype, date_format(log.log_date, '%Y-%m-%d %H:%i:%s') AS request_seconds "
            "           , COUNT(log.log_date) AS request_cnt "
            "       FROM sblog.ad_listing_api_log log "
            "      WHERE log_dt <= date_format(current_date, '%Y-%m-%d') "
            "        AND log_dt >= date_format(current_date - interval '1' day, '%Y-%m-%d') "
            "        AND date_format(log.log_date, '%Y-%m-%d') =  date_format(current_date - interval '1' day, '%Y-%m-%d') "
            "        AND log.env = 'RELEASE' "
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
            "          SELECT request_seconds, ua.servicetype "
            "               , CASE regexp_like(ua.logtype, '.ListingLog$') WHEN true THEN 'Listing' "
            "                                                                         ELSE 'Search' "
            "                  END AS logtype "
            "               , SUM(request_cnt) AS tot_request_cnt "
            "             FROM user_activities ua "
            "         GROUP BY request_seconds, ua.servicetype, regexp_like(ua.logtype, '.ListingLog$') "
            "       ) "
            "     GROUP BY concat(servicetype, ' ', logtype) "
            " ) "
            " ORDER BY title DESC ")

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
                "      WHERE log_dt <= date_format(current_date, '%Y-%m-%d') "
                "        AND log_dt >= date_format(current_date - interval '1' day, '%Y-%m-%d') "
                "        AND date_format(log.log_date, '%Y-%m-%d') =  date_format(current_date - interval '1' day, '%Y-%m-%d') "
                "        AND log.env = 'RELEASE' "
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
                "          SELECT request_per_minute, ua.servicetype "
                "               , CASE regexp_like(ua.logtype, '.ListingLog$') WHEN true THEN 'Listing' "
                "                                                                         ELSE 'Search' "
                "                  END AS logtype "
                "               , SUM(request_cnt) AS tot_request_cnt "
                "             FROM user_activities ua "
                "         GROUP BY request_per_minute, ua.servicetype, regexp_like(ua.logtype, '.ListingLog$') "
                "       ) "
                "     GROUP BY concat(servicetype, ' ', logtype) "
                " ) "
                " ORDER BY title DESC ")


class Chat(object):
    def  __init__(self, webhook_url=TEST_SLACK_WEBHOOK_URL):
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

    def make_attachments(self, message1, message2, message3=None, message4=None, title=None):
        attachments = [{}]
        attachments[0]['color'] = '#36a64f'
        attachments[0]['title'] = title
        attachments[0]["fields"] = [{
                                        "title": "Service",
                                        "value": message1,
                                        "short": "true"
                                    }, {
                                        "title": "RPS",
                                        "value": message2,
                                        "short": "true"
                                    }, {
                                        "title": "Service",
                                        "value": message3,
                                        "short": "true"
                                    }, {
                                        "title": "RPM",
                                        "value": message4,
                                        "short": "true"
                                    }]
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
    queryUtils = QueryUtils()
    slackMessageUtils = SlackMessageUtils()

    try:
        # 1. Connect
        woowahanPresto.connect()

        # 2. Execute SQL with Presto
        # 2-1. select today
        presto_today = woowahanPresto.string_date_from_today(-1)
        inquery_date = ''.join(presto_today)
        print(inquery_date)

        # 2-2-1. execute rps query
        rps_query = queryUtils.albatross_daily_max_rps(inquery_date)
        presto_rps_results = woowahanPresto.fetchall(rps_query)
        print(presto_rps_results)

        # 2-2-2. execute rpm query
        rpm_query = queryUtils.albatross_daily_max_rpm(inquery_date)
        presto_rpm_results = woowahanPresto.fetchall(rpm_query)
        print(presto_rpm_results)

        # 3. close connection
        woowahanPresto.close()

        # # 4. Make message for Slack
        rps_response_array = list(presto_rps_results)
        rps_title = slackMessageUtils.convert_list_to_linefeed_string(rps_response_array, 0)
        rps_value = slackMessageUtils.convert_list_to_linefeed_string(rps_response_array, 1)
        rpm_response_array = list(presto_rpm_results)
        rpm_title = slackMessageUtils.convert_list_to_linefeed_string(rpm_response_array, 0)
        rpm_value = slackMessageUtils.convert_list_to_linefeed_string(rpm_response_array, 1)
        # print("rps_title = \n{%s}" % rps_title)
        # print("rps_value = \n{%s}" % rps_value)

        attachments = slackMessageUtils.make_attachments(rps_title
                                                        , rps_value
                                                        , rpm_title
                                                        , rpm_value)

        # # 5. Send message to Slack
        chat.post_slack(text="*`Albatross MAX(RPS, RPM) 일 통계 SUCCESS: {0}`*".format(inquery_date), attachments=attachments)
        # print("\nattachments = {%s}" % attachments)

    except BaseException as e:
        # send error message to slack
        loggerUtils = LoggerUtils()
        _, _, ex_traceback = sys.exc_info()
        exception_message = "\n".join(loggerUtils.ex_message_traceback(e, ex_traceback))

        print(exception_message)

        chat.post_slack("*`Albatross MAX(RPS, RPM) 일 통계 FAIL`*\n {0}".format(exception_message))
