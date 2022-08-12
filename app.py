import pandas as pd 
import numpy as np 
# from sklearn.linear_model import LogisticRegression
import joblib
import logging
from flask import Flask, jsonify, request, abort, make_response

with open('/project/experimentation/model_artifacts/lm_model.joblib', 'rb') as f:
    predictor = joblib.load(f)
    print("success")

logging.basicConfig(level=logging.INFO)

flask_server = Flask(__name__)

PORT = 8000

@flask_server.route('/predict',methods=['POST'])
def predict():
    # request_body = request.json
    request_body = request.get_json(force=True)
    print(request_body)
    for a,b in request_body.items():
        request_body[a] = [b]
    # print(request_body)
    temp_df = pd.DataFrame(request_body)
    prediction_step1 = predictor.predict(temp_df)
    prediction_step2 = prediction_step1[0]
    if prediction_step2 == 1:
        prediction = "win"
    else:
        prediction = "loss"    
    logging.info('Predicted to {} !'.format(prediction))
    return jsonify({"Most likely to": prediction})

if __name__ == '__main__':
    logging.info('Listening on port {}'.format(PORT))
    flask_server.run(debug=True, host='127.0.0.1', port=PORT)
    