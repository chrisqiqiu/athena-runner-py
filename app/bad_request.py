class BadRequest(Exception):
    """Represents an error and contains specific data about the error."""

    def __init__(self, response, extraMessage):
        self.response = response
        self.extraMessage = extraMessage

    def __str__(self):
        return "The request responded with the following error - %s: %s. Extra detail: %s" % (self.response.status_code, self.response.reason, self.extraMessage)


class ClientError(BadRequest):
    def __init__(self, response):
        super().__init__(response, "Client side Error: " + response.text)


class ServerError(BadRequest):
    def __init__(self, response):
        super().__init__(response, "Server side Error:" + response.text)


class NotFound(BadRequest):
    def __init__(self, response):
        super().__init__(response, "Not found Error:" + response.text)


class Unauthorized(BadRequest):
    def __init__(self, response):
        super().__init__(response,
                         "Unauthorized request or credential is invalid. Extra detail:" + response.text)


class ExpiredOAuthToken(BadRequest):
    def __init__(self, response):
        super().__init__(response, "Authentication could be timeout. Extra detail:" + response.text)


class MultipleQueries(BadRequest):
    def __init__(self, response):
        super().__init__(response, "Precondition Failed. Extra detail:" + response.text)
