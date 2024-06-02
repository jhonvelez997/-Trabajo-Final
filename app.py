from pymongo import MongoClient
from bson import json_util
import json
import re
import pandas as pd
from flask import Flask,request,render_template, jsonify, url_for


client = MongoClient('mongodb://jhonalevc:3004222950a.@172.191.160.139:27017/?tls=false')
db = client['final'] 
collection_ratings = db["ratings"]
collection_movies = db["movies"]
collection_users = db["users"]
collection_finalrdd = db["collection_finalrdd"]
collection_use_rat = db["use_rat"]
collection_dict = db["dict"]

dict_ = list(collection_dict.find())
dict_df = pd.json_normalize(dict_)


app = Flask(__name__)

@app.route("/")
def home():
    return  render_template("index.html")


@app.route("/RATE_TOP20")
def rate_top_20():
    yr = request.args.get("year")
    genre = request.args.get("genre")

    if yr == None and genre == None:
        return jsonify({"Respuesta":"No ingresaste ninguno de los parametros/Argumentos de busqueda"})
    
    if yr == None and genre != None:
        genre = genre.lower().strip()
        response = collection_finalrdd.find({"genres":genre}).sort([("rating",-1)]).limit(20)
        r = list(response)
        if len(r) == 0 : return jsonify({"Respuesta":"No hay Peliculas con su argumento de busqueda"})
        else:
            return jsonify(json_util._json_convert(r))
        
    if yr != None and genre == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
            response = collection_finalrdd.find({"year":yr}).sort([("rating",-1)]).limit(20)
            r = list(response)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para el criterio de busqueda no hay peliculas: {yr}, Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(json_util._json_convert(r))
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})

    if yr !=None and genre != None:
        genre = genre.lower().strip()
        try:
            yr = int(yr.replace(",","").replace(".",""))
            response = collection_finalrdd.find({"year":yr, "genres":genre}).sort([("rating",-1)]).limit(20)
            r = list(response)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para los criterios de busqueda no hay peliculas: {yr},{genre}; Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(json_util._json_convert(r))
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})
    

@app.route("/RATE_BOTTOM20")
def RATE_BOTTOM20():
    yr = request.args.get("year")
    genre = request.args.get("genre")

    if yr == None and genre == None:
        return jsonify({"Respuesta":"No ingresaste ninguno de los parametros/Argumentos de busqueda"})
    
    if yr == None and genre != None:
        genre = genre.lower().strip()
        response = collection_finalrdd.find({"genres":genre}).sort([("rating",1)]).limit(20)
        r = list(response)
        if len(r) == 0 : return jsonify({"Respuesta":"No hay Peliculas con su argumento de busqueda"})
        else:
            return jsonify(json_util._json_convert(r))
        
    if yr != None and genre == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
            response = collection_finalrdd.find({"year":yr}).sort([("rating",1)]).limit(20)
            r = list(response)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para el criterio de busqueda no hay peliculas: {yr}, Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(json_util._json_convert(r))
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})

    if yr !=None and genre != None:
        genre = genre.lower().strip()
        try:
            yr = int(yr.replace(",","").replace(".",""))
            response = collection_finalrdd.find({"year":yr, "genres":genre}).sort([("rating",1)]).limit(20)
            r = list(response)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para los criterios de busqueda no hay peliculas: {yr},{genre}; Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(json_util._json_convert(r))
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})


@app.route("/COUNT_TOP20")
def COUNT_TOP20():
    yr = request.args.get("year")
    genre = request.args.get("genre")

    if yr == None and genre == None:
        return jsonify({"Respuesta":"No ingresaste ninguno de los parametros/Argumentos de busqueda"})
    
    if yr == None and genre != None:
        genre = genre.lower().strip()
        response = collection_finalrdd.find({"genres":genre}).sort([("no_reviews",-1)]).limit(20)
        r = list(response)
        if len(r) == 0 : return jsonify({"Respuesta":"No hay Peliculas con su argumento de busqueda"})
        else:
            return jsonify(json_util._json_convert(r))
        
    if yr != None and genre == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
            response = collection_finalrdd.find({"year":yr}).sort([("no_reviews",-1)]).limit(20)
            r = list(response)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para el criterio de busqueda no hay peliculas: {yr}, Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(json_util._json_convert(r))
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})

    if yr !=None and genre != None:
        genre = genre.lower().strip()
        try:
            yr = int(yr.replace(",","").replace(".",""))
            response = collection_finalrdd.find({"year":yr, "genres":genre}).sort([("no_reviews",-1)]).limit(20)
            r = list(response)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para los criterios de busqueda no hay peliculas: {yr},{genre}; Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(json_util._json_convert(r))
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})


@app.route("/MOVIE")
def MOVIE():
    mv = request.args.get("movie")

    if mv == None:
        return jsonify({"Respuesta":"No ingresaste ningun parametro de busqueda en la peticion"})
    
    if mv != None :
        mv = mv.lower().strip()
        response = collection_finalrdd.find({"name":{"$regex":mv}}).sort([("rating",-1)]).limit(20)
        r = list(response)
        if len(r) == 0 : return jsonify({"Respuesta":"No hay peliculas con ese nombre, revisa el nombre e intenta de nuevo"})
        return jsonify(json_util._json_convert(r))


@app.route("/LISTBYGENDER")
def LISTBYGENDER():
    gender = request.args.get("gender")

    if gender == None:
        return jsonify({"Respuesta":"No ingresaste ningun parametro de busqueda en la peticion"})
    
    gender = gender.lower().strip()

    reponse_vista =  collection_finalrdd.find({"genres":gender}).sort([("no_reviews",-1)]).limit(20)
    resposne_rating =  collection_finalrdd.find({"genres":gender}).sort([("rating",-1)]).limit(20)
    r1_rating = list(resposne_rating)
    r1_vistas = list(reponse_vista)

    if r1_rating == []:
        return jsonify({"Respuesta":f"Para el argumento ingresado {gender}, no hay peliculas"})
    
    else:
        dct = {
            "Mas Vistas": json_util._json_convert(r1_vistas),
            "Mejor Rating": json_util._json_convert(r1_rating)
        }
        return jsonify(dct)



@app.route("/SUGGEST")
def SUGGEST():
    genero = request.args.get("genero")
    edad = request.args.get("edad")


    if genero == None and edad == None:
        return jsonify({"Respuesta":"No ingresaste ningun argumento de busqueda a la petcion"})
    
    elif genero == None and edad != None:
        try: 
            edad = int(edad)
            if edad <1  or edad >100:
                return jsonify({"Respuesta":"El rango de edad debe estar entre 1 y 56"})
            else:
                if edad < 18 :
                    edad = 1
                elif edad >= 18 and edad <24:
                    edad = 18
                elif edad >=25 and edad < 34:
                    edad = 25
                elif edad >= 35 and edad < 45:
                    edad = 35
                elif edad >= 45 and edad <=49:
                    edad = 45
                elif edad >= 50 and edad <= 55:
                    edad = 50
                else:
                    edad = 56
                print(edad)


                response  =  collection_use_rat.find({"age":edad})
                r = list(response)
                r_df = pd.json_normalize(r)
                r_final = r_df.merge(dict_df, how="left", on= "movie_id")
                xc = r_final.groupby("movie_id")["rating"].agg(["mean","count"]).reset_index().merge(
                    r_final[["movie_id","name","year"]],
                    how="left",
                    on = "movie_id"
                ).drop_duplicates(subset="movie_id")
                xc.columns = ["movie_id","rating_promedio","No_Reviews","Name","year"]
                xc = xc.sort_values(by="No_Reviews", ascending=False).head(20).to_json(orient="records")
                return jsonify(json.loads(xc))
        except:
            return jsonify({"Respuesta":f"El argumento ingresado para la busqueda edad {edad} no es numerico"})
        
    elif genero != None and edad == None:
        genero = genero.upper().strip()
        if genero not in ["M","F"]:
            return jsonify({"Respuesta":f"El argumento de busqueda genero pasado {genero} debe ser F para femenino y M para masculino"})
        else:

            response  =  collection_use_rat.find({"user_gender":genero})
            r = list(response)
            r_df = pd.json_normalize(r)
            r_final = r_df.merge(dict_df, how="left", on= "movie_id")
            xc = r_final.groupby("movie_id")["rating"].agg(["mean","count"]).reset_index().merge(
                r_final[["movie_id","name","year"]],
                how="left",
                on = "movie_id"
            ).drop_duplicates(subset="movie_id")
            xc.columns = ["movie_id","rating_promedio","No_Reviews","Name","year"]
            xc = xc.sort_values(by="No_Reviews", ascending=False).head(20).to_json(orient="records")
            return jsonify(json.loads(xc))

        
    elif genero != None and edad != None:
        genero = genero.upper().strip()
        if genero not in ["M","F"]:
            return jsonify({"Respuesta":f"El argumento de busqueda genero pasado {genero} debe ser F para femenino y M para masculino"})

        try: 
            edad = int(edad)
            if edad <1  or edad >100:
                return jsonify({"Respuesta":"El rango de edad debe estar entre 1 y 56"})
            else:
                if edad < 18 :
                    edad = 1
                elif edad >= 18 and edad <24:
                    edad = 18
                elif edad >=25 and edad < 34:
                    edad = 25
                elif edad >= 35 and edad < 45:
                    edad = 35
                elif edad >= 45 and edad <=49:
                    edad = 45
                elif edad >= 50 and edad <= 55:
                    edad = 50
                else:
                    edad = 56
                print(edad)

                response  =  collection_use_rat.find({"age":edad, "user_gender":genero})
                r = list(response)
                if len(r) == 0:
                    return jsonify({"Respuesta":"No hay datos para tus argumentos de busqueda"})
                r_df = pd.json_normalize(r)
                r_final = r_df.merge(dict_df, how="left", on= "movie_id")
                xc = r_final.groupby("movie_id")["rating"].agg(["mean","count"]).reset_index().merge(
                    r_final[["movie_id","name","year"]],
                    how="left",
                    on = "movie_id"
                ).drop_duplicates(subset="movie_id")
                xc.columns = ["movie_id","rating_promedio","No_Reviews","Name","year"]
                xc = xc.sort_values(by="No_Reviews", ascending=False).head(20).to_json(orient="records")
                return jsonify(json.loads(xc))

        except:
            return jsonify({"Respuesta":f"El argumento ingresado para la busqueda edad {edad} no es numerico"})
        


if __name__ == "__main__":
    app.run()
