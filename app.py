import findspark
from pyspark.sql import SparkSession
import re
from flask import Flask,request,render_template, jsonify, url_for
findspark.init()

spark = SparkSession.builder.appName("Trabajo_Final").master("local[*]").getOrCreate()
sc = spark.sparkContext


def extract_util(text):
    pattern = r'\((.*?)\)'  
    matches = re.findall(pattern, text)  
    return matches


dict_users = {}
with open(r"ml-1m/users.dat", encoding="latin-1") as file:
    lines = file.readlines()
    for line in lines:
        dat = line.split("::")
        dict_users[dat[0]] = (dat[1], int(dat[2]))
dict_users_bd = sc.broadcast(dict_users)

dict_meta = {}
with open(r"ml-1m/movies.dat", encoding="latin-1") as file:
    lines = file.readlines()
    for line in lines:
        dat = line.split("::")

        dict_meta[dat[0]] = (dat[1], dat[2].replace("\n",""))
dict_meta_bd = sc.broadcast(dict_meta)


use_rat = sc.textFile(r"ml-1m/ratings.dat") \
    .map(lambda x: x.split("::")) \
    .map(lambda x: [x[1], int(x[2]), x[0]]) \
    .map(lambda x : (x[0],(x[1],x[2]), dict_users_bd.value[x[-1]]))

ratings = sc.textFile(r"ml-1m/ratings.dat") \
    .map(lambda x: x.split("::")) \
    .map(lambda x: [x[1], int(x[2])]) \
    .mapValues(lambda x: (x,1)) \
    .reduceByKey(lambda x,y : (x[0] +y[0], x[1]+ y[1])) \
    .mapValues(lambda x : (round(x[0]/x[1],2), x[1]))

final_rdd = ratings.map(lambda x: (x[0], x[1], dict_meta_bd.value[x[0]])) \
    .map(lambda x: (x[0], x[1][0], x[1][1], x[2][0], x[2][1])) \
    .map(lambda x: (
        x[0],
        x[1],
        x[2], 
        re.sub(r'\((\d+)\)', "" ,x[3] ).lower().strip(), 
        int(re.findall(r'\((\d+)\)' , x[3])[0]) , 
        x[4].lower().split("|")))

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
        r = final_rdd \
            .filter(lambda x: genre in x[5]) \
            .sortBy(lambda x : -x[1]) \
            .take(20)
        return jsonify(r)
    
    if yr != None and genre == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
            r = final_rdd \
                .filter(lambda x : x[4]== yr) \
                .sortBy(lambda x : -x[1]) \
                .collect()
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para el criterio de busqueda no hay peliculas: {yr}, Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(r)
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})
        
    if yr !=None and genre != None:
        genre = genre.lower().strip()
        try:
            yr = int(yr.replace(",","").replace(".",""))
            r = final_rdd \
                .filter(lambda x : x[4]== yr) \
                .filter(lambda x: genre in x[5]) \
                .sortBy(lambda x : -x[1]) \
                .take(20)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para los criterios de busqueda no hay peliculas: {yr},{genre}; Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(r)
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
        r = final_rdd \
            .filter(lambda x: genre in x[5]) \
            .sortBy(lambda x : x[1]) \
            .take(20)
        return jsonify(r)
    
    if yr != None and genre == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
            r = final_rdd \
                .filter(lambda x : x[4]== yr) \
                .sortBy(lambda x : x[1]) \
                .take(20)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para el criterio de busqueda no hay peliculas: {yr}, Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(r)
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})
        
    if yr !=None and genre != None:
        genre = genre.lower().strip()
        try:
            yr = int(yr.replace(",","").replace(".",""))
            r = final_rdd \
                .filter(lambda x : x[4]== yr) \
                .filter(lambda x: genre in x[5]) \
                .sortBy(lambda x : x[1]) \
                .take(20)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para los criterios de busqueda no hay peliculas: {yr},{genre}; Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(r)
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
        r = final_rdd \
            .filter(lambda x: genre in x[5]) \
            .sortBy(lambda x : -x[2]) \
            .take(20)
        return jsonify(r)
    
    if yr != None and genre == None:
        try:
            yr = int(yr.replace(",","").replace(".",""))
            r = final_rdd \
                .filter(lambda x : x[4]== yr) \
                .sortBy(lambda x : -x[2]) \
                .take(20)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para el criterio de busqueda no hay peliculas: {yr}, Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(r)
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})
        
    if yr !=None and genre != None:
        genre = genre.lower().strip()
        try:
            yr = int(yr.replace(",","").replace(".",""))
            r = final_rdd \
                .filter(lambda x : x[4]== yr) \
                .filter(lambda x: genre in x[5]) \
                .sortBy(lambda x : -x[2]) \
                .take(20)
            if len(r) == 0:
                return jsonify({"Respuesta":f"Para los criterios de busqueda no hay peliculas: {yr},{genre}; Nota: El año maximo es 2000 y el minimo 1919"})
            return jsonify(r)
        except:
            return jsonify({"Respuesta":f"El parametro que ingresaste (year) no es numerico: {yr}"})


@app.route("/MOVIE")
def MOVIE():
    mv = request.args.get("movie")

    if mv == None:
        return jsonify({"Respuesta":"No ingresaste ningun parametro de busqueda en la peticion"})
    
    if mv != None :
        mv = mv.lower().strip()
        r = final_rdd \
            .filter(lambda x: mv in x[3]) \
            .take(5)
        if len(r) == 0 :
            return jsonify({"Respuesta":"No hay peliculas con ese nombre, revisa el nombre e intenta de nuevo"})
        return jsonify(r)
    

@app.route("/LISTBYGENDER")
def LISTBYGENDER():
    gender = request.args.get("gender")

    if gender == None:
        return jsonify({"Respuesta":"No ingresaste ningun parametro de busqueda en la peticion"})
    
    gender = gender.lower().strip()

    r1_vistas = final_rdd \
        .filter(lambda x: gender in x[5]) \
        .sortBy(lambda x : -x[2]).take(5)
    r1_rating = final_rdd \
        .filter(lambda x: gender in x[5]) \
        .sortBy(lambda x : -x[1]).take(5)
    
    if r1_rating == []:
        return jsonify({"Respuesta":f"Para el argumento ingresado {gender}, no hay peliculas"})
    
    else:
        dct = {
            "Mas Vistas": r1_vistas,
            "Mejor Rating": r1_rating
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
                r = use_rat.filter(lambda x: x[2][1] == edad) \
                        .map(lambda x : (x[0], (x[1][0], 1))) \
                        .reduceByKey( lambda x, y : (x[0] + y[0], x[1]+ y[1])) \
                        .mapValues(lambda x: (round(x[0]/x[1],2))) \
                        .map(lambda x : (x[0], (x[1]), (dict_meta_bd.value[x[0]]))) \
                        .map(lambda x : (
                            x[0],
                            x[1],
                            re.sub(r'\((\d+)\)', "" ,x[2][0] ).lower().strip(),
                            int(re.findall(r'\((\d+)\)' , x[2][0])[0]),
                            x[2][1].lower().split("|")
                            )
                        ) \
                        .sortBy(lambda x : -x[1]).take(25) 
                return jsonify(r)
        except:
            return jsonify({"Respuesta":f"El argumento ingresado para la busqueda edad {edad} no es numerico"})
        
    elif genero != None and edad == None:
        genero = genero.upper().strip()
        if genero not in ["M","F"]:
            return jsonify({"Respuesta":f"El argumento de busqueda genero pasado {genero} debe ser F para femenino y M para masculino"})
        else:
            r = use_rat.filter(lambda x: x[2][0] == genero) \
                            .map(lambda x : (x[0], (x[1][0], 1))) \
                            .reduceByKey( lambda x, y : (x[0] + y[0], x[1]+ y[1])) \
                            .mapValues(lambda x: (round(x[0]/x[1],2))) \
                            .map(lambda x : (x[0], (x[1]), (dict_meta_bd.value[x[0]]))) \
                            .map(lambda x : (
                                x[0],
                                x[1],
                                re.sub(r'\((\d+)\)', "" ,x[2][0] ).lower().strip(),
                                int(re.findall(r'\((\d+)\)' , x[2][0])[0]),
                                x[2][1].lower().split("|")
                                )
                            ) \
                            .sortBy(lambda x : -x[1]).take(25) 
            
            return jsonify(r)
        
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
                r = use_rat.filter(lambda x: x[2][0] == genero) \
                        .filter(lambda x: x[2][1] == edad) \
                        .map(lambda x : (x[0], (x[1][0], 1))) \
                        .reduceByKey( lambda x, y : (x[0] + y[0], x[1]+ y[1])) \
                        .mapValues(lambda x: (round(x[0]/x[1],2))) \
                        .map(lambda x : (x[0], (x[1]), (dict_meta_bd.value[x[0]]))) \
                        .map(lambda x : (
                            x[0],
                            x[1],
                            re.sub(r'\((\d+)\)', "" ,x[2][0] ).lower().strip(),
                            int(re.findall(r'\((\d+)\)' , x[2][0])[0]),
                            x[2][1].lower().split("|")
                            )
                        ) \
                        .sortBy(lambda x : -x[1]).take(25) 
                
                if len(r) == 0:
                    return jsonify({"Respuesta":"No hay datos para tus argumentos de busqueda"})
                return jsonify(r)
        except:
            return jsonify({"Respuesta":f"El argumento ingresado para la busqueda edad {edad} no es numerico"})
        


if __name__ == "__main__":
    app.run()
