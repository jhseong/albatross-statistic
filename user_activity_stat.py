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
SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'
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

    def today(self):
        return self.fetchone("SELECT date_format(current_date, '%Y-%m-%d')")

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

    def make_attachments(self, message1, message2, date=None):
        attachments = [{}]
        inquery_date = date
        if date is None:
            inquery_date = self.__string_date_format(date=datetime.utcnow() - timedelta(0))

        attachments[0]['color'] = '#36a64f'
        attachments[0]['title'] = "Baemin/Riders Daily Request Summary(Listing/Search): {0}".format(inquery_date)
        attachments[0]["fields"] = [{
                                        "value": message1,
                                        "short": "true"
                                    }, {
                                        "value": message2,
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

    try:
        woowahanPresto = WoowahanPresto()
        queryUtils = QueryUtils()
        slackMessageUtils = SlackMessageUtils()

        # 1. Connect
        woowahanPresto.connect()

        # 2. Execute SQL with Presto
        # 2-1. select today
        presto_today = woowahanPresto.today()
        inquery_date = ''.join(presto_today)
        print(inquery_date)

        # 2-2. execute query
        query = queryUtils.albatross_daily_statistic(inquery_date)
        presto_results = woowahanPresto.fetchall(query)
        woowahanPresto.close()

        # 3. Make message for Slack
        response_array = list(presto_results)
        request_type_value = slackMessageUtils.convert_list_to_linefeed_string(response_array, 0)
        total_value = slackMessageUtils.convert_list_to_linefeed_string(response_array, 1)
        print("request_type_value = \n{%s}" % request_type_value)
        print("total = \n{%s}" % total_value)

        attachments = slackMessageUtils.make_attachments(request_type_value, total_value, inquery_date)

        # 4. Send message to Slack
        chat.post_slack(text="`Albatross 일 통계 SUCCESS`", attachments=attachments)
        print("\nattachments = {%s}" % attachments)

    except BaseException as e:
        # send error message to slack
        loggerUtils = LoggerUtils()
        _, _, ex_traceback = sys.exc_info()
        exception_message = "\n".join(loggerUtils.ex_message_traceback(e, ex_traceback))

        print(exception_message)

        chat.post_slack("`Albatross 일 통계 Alert FAIL`\n {0}".format(exception_message))
