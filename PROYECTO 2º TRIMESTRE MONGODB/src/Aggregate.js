db.coches.aggregate([
{
    $match:{
        Año:{$gt:2010},
        SegundaMano: false
    }
},
{
    $project:{
        Modelo: "$Modelo",
        Año: "$Año",
        Combustible: "$Combustible",
        Consumo: "$Consumo",
        Color: "$Color",
        Plazas: "$Plazas",
        LugaresDeVenta: "$LugaresDeVenta",
        PotenciaMaxima: "$PotenciaMaxima",
        Iva: {$round: [{$multiply: ["$Precio", 0.21]}, 0]},
        PrecioIvaIncluido:{$round: [{$multiply: ["$Precio",1.21]},0]},
        CodFabricante: "$CodFabricante",
    }
},
{
    $lookup:{
        from:"fabricante",
        localField:"CodFabricante",
        foreignField:"Codigo",
        as:"InformacionDeMarca",
    }
},
{
    $project:{
        Modelo: "$Modelo",
        Año: "$Año",
        Combustible: "$Combustible",
        Consumo: "$Consumo",
        Color: "$Color",
        Plazas: "$Plazas",
        LugaresDeVenta: "$LugaresDeVenta",
        PotenciaMaxima: "$PotenciaMaxima",
        PrecioIvaIncluido: "$PrecioIvaIncluido",
        Porcentaje:{$round: [{$multiply: ["$PrecioIvaIncluido",
                                          { $arrayElemAt: ["$InformacionDeMarca.Descuento", 0]}]},0]},
        InformacionDeMarca: "$InformacionDeMarca" 
            }       
},
{
    $project:{
        Modelo: "$Modelo",
        Año: "$Año",
        Combustible: "$Combustible",
        Consumo: "$Consumo",
        Color: "$Color",
        Plazas: "$Plazas",
        PotenciaMaxima: "$PotenciaMaxima",
        PrecioIvaIncluido: "$PrecioIvaIncluido",
        DineroDescuento: "$Porcentaje",
        Contacto: ["$InformacionDeMarca.Numerodeayuda","$InformacionDeMarca.Email"]
    }
}
]).pretty()
/*QUEREMOS SABER EL PRECIO CON IVA, EL DINERO QUE NOS VA A DESCONTAR LA MARCA 
Y CONTACTO CON LA MARCA DE LOS COCHES ADEMAS DE SUS CARACTERISTICAS.
EL CLIENTE QUIERE UN COCHE NUEVO Y POSTERIOR AL 2010*/

db.coches.aggregate([
    {
        $unwind: "$LugaresDeVenta"
    },
    {
        $match:{
            LugaresDeVenta: "Sevilla",
            SegundaMano: true
        }      
    },
    {
        $lookup:{
            from:"fabricante",
            localField:"CodFabricante",
            foreignField:"Codigo",
            as:"InformacionDeMarca",
        }
    },
    {
        $group: {
            _id : {
                Modelo: "$Modelo",
                Combustible: "$Combustible",
                Kilometros: "$Kms",
                Color: "$Color",
                Precio: "$Precio",
                Iva: {$round: [{$multiply: ["$Precio", 0.21]}, 0]},
                PrecioIvaIncluido:{$round: [{$multiply: ["$Precio",1.21]},0]}
            },
            InformacionDeMarca: {
                $push: {
                    Email: "$InformacionDeMarca.Email",
                    TelefonoFabricante: "$InformacionDeMarca.Numerodeayuda"
                }
            }
        }
    },
    ]).pretty()
    /*QUEREMOS SABER LOS COCHES DISPONIBLES EN SEVILLA DE SEGUNDA MANO, SUS CARACTERISTICAS,
     EL PRECIO SIN IVA, LO QUE PAGAMOS DE IVA , EL PRECIO FINAL Y ALGUN TIPO DE CONTACTO CON EL FABRICANTE */
     db.coches.aggregate([
        {
            $match:{$expr: {$lte:["$Consumo.Medio",4.5]} }
        },
        {
            $project:{
                Modelo: "$Modelo",
                Año: "$Año",
                Combustible: "$Combustible",
                Consumo: "$Consumo",
                Color: "$Color",
                Plazas: "$Plazas",
                LugaresDeVenta: "$LugaresDeVenta",
                PotenciaMaxima: "$PotenciaMaxima",
                Iva: {$round: [{$multiply: ["$Precio", 0.21]}, 0]},
                PrecioIvaIncluido:{$round: [{$multiply: ["$Precio",1.21]},0]},
                CodFabricante: "$CodFabricante",
            }
        },
        {
            $group: {
                _id:  "$CodFabricante",
                MediaDePrecio:{
                    $avg: "$PrecioIvaIncluido"
                }
            }
        },
        {
            $lookup:{
                from:"fabricante",
                localField:"_id",
                foreignField:"Codigo",
                as:"Marca",
            }
        },
        {
            $project:{
                MediaDePrecio: "$MediaDePrecio",
                Marca: ["$Marca.Fabricante","$Marca.Sede"]
            }
        },
        {
            $sort: {
                MediaDePrecio: -1
            }
        },
        ]).pretty()
        /*  QUEREMOS SABER LA MEDIA DE PRECIO INCLUYENDO EL IVA, 
        DE LAS MARCAS CON VEHICULOS DISPONIBLES CUYO CONSUMO MEDIO ES MENOR O IGUAL A 4.5 L/100KM, 
        ORDENADAS DE MANERA DESCENDENTE */



        /* RESULTADOS PRIMER AGGREGATE

        {
        "_id" : ObjectId("603e7259c2afb47835a7469f"),
        "Modelo" : "Mercedes-Benz Clase E E 300 de 4p.",
        "Año" : 2020,
        "Combustible" : "Hibrido",
        "Consumo" : {
                "Urbano" : 4,
                "Extraurbano" : 4.2,
                "Medio" : 4.1
        },
        "Color" : "Blanco",
        "Plazas" : 5,
        "PotenciaMaxima" : 306,
        "PrecioIvaIncluido" : 70542,
        "DineroDescuento" : 14108,
        "Contacto" : [
                [
                        "914 846 000"
                ],
                [
                        "cs.esp@cac.mercedes-benz.com"
                ]
        ]
}
{
        "_id" : ObjectId("603e7259c2afb47835a746a0"),
        "Modelo" : "Renault Megane S.T. R.S.Line ETECH",
        "Año" : 2020,
        "Combustible" : "Hibrido",
        "Consumo" : {
                "Urbano" : 3,
                "Extraurbano" : 3,
                "Medio" : 3
        },
        "Color" : "Azul",
        "Plazas" : 5,
        "PotenciaMaxima" : 160,
        "PrecioIvaIncluido" : 39870,
        "DineroDescuento" : 3987,
        "Contacto" : [
                [
                        "915 065 358"
                ],
                [
                        "atencioncliente@renault.es"
                ]
        ]
}
{
        "_id" : ObjectId("603e7259c2afb47835a746a1"),
        "Modelo" : "Alfa Romeo Stelvio 2.0",
        "Año" : 2020,
        "Combustible" : "Gasolina",
        "Consumo" : {
                "Urbano" : 8.9,
                "Extraurbano" : 5.9,
                "Medio" : 7
        },
        "Color" : "Rojo",
        "Plazas" : 5,
        "PotenciaMaxima" : 200,
        "PrecioIvaIncluido" : 66890,
        "DineroDescuento" : 3344,
        "Contacto" : [
                [
                        "800 2532 0000"
                ],
                [
                        "atencioncliente@alfaromeo.es"
                ]
        ]
}
{
        "_id" : ObjectId("603e7259c2afb47835a746a2"),
        "Modelo" : "BMW Serie 2 218dA Gran Coupe 4p.",
        "Año" : 2021,
        "Combustible" : "Diesel",
        "Consumo" : {
                "Urbano" : 6.2,
                "Extraurbano" : 3.8,
                "Medio" : 4.2
        },
        "Color" : "Negro",
        "Plazas" : 5,
        "PotenciaMaxima" : 150,
        "PrecioIvaIncluido" : 50699,
        "DineroDescuento" : 7605,
        "Contacto" : [
                [
                        "900 357 902"
                ],
                [
                        "rclientes@bmw.es"
                ]
        ]
}
{
        "_id" : ObjectId("603e7259c2afb47835a746a9"),
        "Modelo" : "Kia Ceed Tourer Tourer 1.0 TGDi 88kW 120CV Tech 5p",
        "Año" : 2021,
        "Combustible" : "Gasolina",
        "Consumo" : {
                "Urbano" : 6.5,
                "Extraurbano" : 5,
                "Medio" : 5.7
        },
        "Color" : "Azul",
        "Plazas" : 5,
        "PotenciaMaxima" : 120,
        "PrecioIvaIncluido" : 23958,
        "DineroDescuento" : 5990,
        "Contacto" : [
                [
                        "663 954 877"
                ],
                [
                        "contacto@kia.es"
                ]
        ]
}
{
        "_id" : ObjectId("603e7259c2afb47835a746ae"),
        "Modelo" : "Citroen C3 Aircross PureTech 96kW 130CV SS EAT6 SHINE 5p",
        "Año" : 2020,
        "Combustible" : "Gasolina",
        "Consumo" : {
                "Urbano" : 5.9,
                "Extraurbano" : 4.5,
                "Medio" : 5
        },
        "Color" : "Blanco",
        "Plazas" : 5,
        "PotenciaMaxima" : 131,
        "PrecioIvaIncluido" : 24079,
        "DineroDescuento" : 3612,
        "Contacto" : [
                [
                        "913 213 921"
                ],
                [
                        "clientes-internet@citroen.com"
                ]
        ]
}
{
        "_id" : ObjectId("603e7259c2afb47835a746b5"),
        "Modelo" : "KIA Sportage 1.6 MHEV Concept 85kW 115CV 4x2 5p",
        "Año" : 2020,
        "Combustible" : "Hibrido",
        "Consumo" : {
                "Urbano" : 4.2,
                "Extraurbano" : 4.1,
                "Medio" : 4.2
        },
        "Color" : "Blanco",
        "Plazas" : 5,
        "PotenciaMaxima" : 115,
        "PrecioIvaIncluido" : 28314,
        "DineroDescuento" : 7078,
        "Contacto" : [
                [
                        "663 954 877"
                ],
                [
                        "contacto@kia.es"
                ]
        ]
} */



/*RESULTADOS SEGUNDO AGGREGATE 

{
        "_id" : {
                "Modelo" : "Skoda Fabia Comfort 1.4",
                "Combustible" : "Diesel",
                "Kilometros" : 80000,
                "Color" : "Gris",
                "Precio" : 2500,
                "Iva" : 525,
                "PrecioIvaIncluido" : 3025
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "atencioncliente@skoda.es"
                        ],
                        "TelefonoFabricante" : [
                                "800 500 103"
                        ]
                }
        ]
}
{
        "_id" : {
                "Modelo" : "Audi A1 Sportback S line 30 TFSI 81kW S tronic 5p.",
                "Combustible" : "Gasolina",
                "Kilometros" : 19200,
                "Color" : "Blanco",
                "Precio" : 23900,
                "Iva" : 5019,
                "PrecioIvaIncluido" : 28919
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "atencioncliente@audi.es"
                        ],
                        "TelefonoFabricante" : [
                                "638 680 102"
                        ]
                }
        ]
}
{
        "_id" : {
                "Modelo" : "Volkswagen Golf Business 1.0 TSI 85kW 115CV 5p",
                "Combustible" : "Gasolina",
                "Kilometros" : 35402,
                "Color" : "Blanco",
                "Precio" : 15800,
                "Iva" : 3318,
                "PrecioIvaIncluido" : 19118
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "atencioncliente@volkswagen.es"
                        ],
                        "TelefonoFabricante" : [
                                "800 500 100"
                        ]
                }
        ]
}
{
        "_id" : {
                "Modelo" : "Renault Scenic CONFORT DYNAMIQUE 1.9DCI 5p",
                "Combustible" : "Diesel",
                "Kilometros" : 240000,
                "Color" : "Gris",
                "Precio" : 1400,
                "Iva" : 294,
                "PrecioIvaIncluido" : 1694
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "atencioncliente@renault.es"
                        ],
                        "TelefonoFabricante" : [
                                "915 065 358"
                        ]
                }
        ]
}
{
        "_id" : {
                "Modelo" : "Opel Mokka X 1.6 CDTi 100kW 4X2 SS Excellence 5p",
                "Combustible" : "Diesel",
                "Kilometros" : 79000,
                "Color" : "Azul",
                "Precio" : 15500,
                "Iva" : 3255,
                "PrecioIvaIncluido" : 18755
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "renting@opel.com"
                        ],
                        "TelefonoFabricante" : [
                                "800 000 921"
                        ]
                }
        ]
}
{
        "_id" : {
                "Modelo" : "BMW X3 XDRIVE20D 5p",
                "Combustible" : "Diesel",
                "Kilometros" : 145000,
                "Color" : "Gris",
                "Precio" : 53500,
                "Iva" : 11235,
                "PrecioIvaIncluido" : 64735
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "rclientes@bmw.es"
                        ],
                        "TelefonoFabricante" : [
                                "900 357 902"
                        ]
                }
        ]
}
{
        "_id" : {
                "Modelo" : "Ford KA 1.3 3p",
                "Combustible" : "Gasolina",
                "Kilometros" : 134222,
                "Color" : "Rojo",
                "Precio" : 1950,
                "Iva" : 410,
                "PrecioIvaIncluido" : 2360
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "atencioncliente@ford.es"
                        ],
                        "TelefonoFabricante" : [
                                "900 807 090"
                        ]
                }
        ]
}
{
        "_id" : {
                "Modelo" : "Ford Focus 1.6 TDCi 115cv Titanium Sportbreak 5p",
                "Combustible" : "Diesel",
                "Kilometros" : 99.99,
                "Color" : "Rojo",
                "Precio" : 8790,
                "Iva" : 1846,
                "PrecioIvaIncluido" : 10636
        },
        "InformacionDeMarca" : [
                {
                        "Email" : [
                                "atencioncliente@ford.es"
                        ],
                        "TelefonoFabricante" : [
                                "900 807 090"
                        ]
                }
        ]
}

/* RESULTADOS TERCER AGGREGATE

{
        "_id" : "MB",
        "MediaDePrecio" : 70542,
        "Marca" : [
                [
                        "Mercedes-Benz"
                ],
                [
                        "Alemania"
                ]
        ]
}
{
        "_id" : "BMW",
        "MediaDePrecio" : 50699,
        "Marca" : [
                [
                        "BMW"
                ],
                [
                        "Alemania"
                ]
        ]
}
{
        "_id" : "RE",
        "MediaDePrecio" : 39870,
        "Marca" : [
                [
                        "Renault"
                ],
                [
                        "Francia"
                ]
        ]
}
{
        "_id" : "KIA",
        "MediaDePrecio" : 28314,
        "Marca" : [
                [
                        "KIA"
                ],
                [
                        "Corea del Sur"
                ]
        ]
}
{
        "_id" : "FO",
        "MediaDePrecio" : 14968,
        "Marca" : [
                [
                        "Ford"
                ],
                [
                        "Estados Unidos"
                ]
        ]
}
{
        "_id" : "TY",
        "MediaDePrecio" : 11979,
        "Marca" : [
                [
                        "Toyota"
                ],
                [
                        "Japon"
                ]
        ]
}
{
        "_id" : "SK",
        "MediaDePrecio" : 11495,
        "Marca" : [
                [
                        "Skoda"
                ],
                [
                        "Republica Checa"
                ]
        ]
}
{
        "_id" : "SE",
        "MediaDePrecio" : 10527,
        "Marca" : [
                [
                        "Seat"
                ],
                [
                        "España"
                ]
        ]
}
{
        "_id" : "OP",
        "MediaDePrecio" : 8458,
        "Marca" : [
                [
                        "Opel"
                ],
                [
                        "Alemania"
                ]
        ]
}
{
        "_id" : "CI",
        "MediaDePrecio" : 1694,
        "Marca" : [
                [
                        "Citroen"
                ],
                [
                        "Francia"
                ]
        ]
} */
