
class NotFound(object):
    def get(self, k, d=None):
        if d == self:
            return d
        return ''


not_found = NotFound()