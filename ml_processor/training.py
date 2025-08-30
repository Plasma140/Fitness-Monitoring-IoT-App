import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
from sklearn.model_selection import train_test_split
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier # neural network
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from flask import Flask, request, jsonify
import joblib
import gzip

app = Flask(__name__)
@app.route("/processar_dados/", methods=["POST"])
def processar_dados():
    dados = request.json  # Recebe dados em JSON
    
    # Converte o JSON para um DataFrame do pandas
    df = pd.DataFrame([dados])
    
    file_path = "/app/dados_processados.csv"
    df.to_csv(file_path, sep=';', index=False)
    
    
    # Caminho para o modelo treinado
    model_path = "/app/model/model-DT.dat.gz"

    # Carregar o modelo treinado
    model = joblib.load(model_path)
    print("Modelo carregado com sucesso!")
    
    # load the online dataset
    activity_data = pd.read_csv(file_path, sep=';')
    
    # Garantir que as colunas estão na ordem correta (conforme os dados de treino)
    X = activity_data.to_numpy()
    
    # Fazer predições
    prediction = model.predict(X)
    print(prediction[0])
    
    # Exibir os resultados
    #for i, prediction in enumerate(predictions):
    #    activity = "Correr" if prediction == 1 else "Andar"
    #    print(f"Exemplo {i + 1}: Activity = {activity}")
    
    return jsonify(str(prediction))  # Retorna os dados processados

#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#

@app.route("/training_data/", methods=["POST"])
def training():
    dados = request.json["data"]  # Recebe dados em JSON
    
    # Converte o JSON para um DataFrame do pandas
    df = pd.DataFrame(dados)
    
    # Caminho para o arquivo no volume compartilhado
    file_path = "/app/training_data.csv"
    df.to_csv(file_path, sep=';', index=False)

    # load the training dataset
    activity_data = pd.read_csv(file_path, sep=';')

    missing_value_describe(activity_data)

    # dimension
    print("the dimension:", activity_data.shape)


    # we will split data to 80% training data and 20% testing data with random seed of 10
    activity_data = activity_data.drop(['date', 'time'], axis=1)
    X = activity_data[['acceleration_x', 'acceleration_y', 'acceleration_z','gyro_x', 'gyro_y', 'gyro_z']]
    Y = activity_data['activity']

    print("Dataset dimensions:", activity_data.shape)

    # class distribution
    print(activity_data.groupby('activity').size())

    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=7)

    print("X_train.shape:", X_train.shape)
    print("X_test.shape:", X_test.shape)
    print("Y_train.shape:", X_train.shape)
    print("Y_test.shape:", Y_test.shape)

    models = []                            #Linear Discriminant Analysis

    # nonlinear models
    models.append(('DT', DecisionTreeClassifier()))  #decision tree classifier
    
    # evaluate each model in turn
    print("Model Accuracy:")
    names = []
    accuracy = []
    for name, model in models:
        # 10 fold cross validation to evalue model
        kfold = KFold(n_splits=10, shuffle=True)
        cv_results = cross_val_score(model, X_train, Y_train, cv=kfold, scoring='accuracy')

        # display the cross validation results of the current model
        names.append(name)
        accuracy.append(cv_results)
        msg = "%s: accuracy=%f std=(%f)" % (name, cv_results.mean(), cv_results.std())
        print(msg)

    # predict values with our test set
    for name, model in models:
        print("----------------")
        print("Testing", name)
        test_model(name, model, X_train, Y_train, X_test, Y_test)
    
    return jsonify("OK")  # Retorna os dados processados

#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#

# my personal reusable function for detecting missing data
def missing_value_describe(data):
    # check missing values in training data
    missing_value_stats = (data.isnull().sum() / len(data)*100)
    missing_value_col_count = sum(missing_value_stats > 0)
    missing_value_stats = missing_value_stats.sort_values(ascending=False)[:missing_value_col_count]
    print("Number of columns with missing values:", missing_value_col_count)
    if missing_value_col_count != 0:
        # print out column names with missing value percentage
        print("\nMissing percentage (desceding):")
        print(missing_value_stats)
    else:
        print("No misisng data!!!")
        
#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#

# reusable function to test our model
def test_model(name, model, X_train, Y_train, X_test, Y_test):
    model.fit(X_train, Y_train) # train the whole training set
    
    # Export model
    joblib.dump(model, gzip.open('model/model-' + name + '.dat.gz', "wb"))
    
    
    predictions = model.predict(X_test) # predict on test set
    
    # output model testing results
    print("Accuracy:", accuracy_score(Y_test, predictions))
    print("Confusion Matrix:")
    print(confusion_matrix(Y_test, predictions))
    print("Classification Report:")
    print(classification_report(Y_test, predictions))
    
#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------#
        
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
