from flask_restful import Resource

class Ping(Resource):
    def get(self):
        return {"status": "ok", "version": "0.1"}, 200
