/* RATNI CHAIMA 
immatriculé 201500010292 
G3 M2 IL */

/*---------------------------------------- TP7 ----------------------------------------*/

/*Réponse 1*/
/*Activation des options autotrace et timing de oracle*/
set timing on;
set autotrace on explain;

/*Ecriture et exécution de la requête R1 qui donne le nombre d’aéroports du Portugal*/
alter system flush shared_pool;
alter system flush buffer_cache;

Select count(NumAerD) as NbAeroportDep
From DAeroportDep where NomPays = 'Portugal';

/*Réponse 2*/
/* Création un index b-arbre de la table DAeroportDep sur l’attribut NomPays*/

CREATE Index Index_AeroportDep on DAeroportDep(NomPays) TABLESPACE Master2_tbs ;

/*Réponse 3*/
/*Réexécution de la requête R1*/

alter system flush shared_pool;
alter system flush buffer_cache;

Select count(NumAerD) as NbAeroportDep
From DAeroportDep where NomPays = 'Portugal';

/*Réponse 4*/
/* Suppression de l’index b-arbre*/
Drop Index Index_AeroportDep;

/*création d'index bitmap de la même table et sur le même attribut.*/
Create Bitmap Index Index_AeroportDep on DAeroportDep(NomPays) TABLESPACE Master2_tbs ;

/*Réponse 5*/
/*Réexécution de la requête R1*/
alter system flush shared_pool;
alter system flush buffer_cache;

Select count(NumAerD) as NbAeroportDep
From DAeroportDep where NomPays = 'Portugal';


/*Réponse 6*/
/*Ecriture de la requête R2 qui donne le Nombre de vol en retard à destination des aéroports de type ‘National’*/

Select count(FTraffic.NbVolRetard) as NombreVolRetard
From FTraffic ,DAeroportArr
where FTraffic.NumAerA = DAeroportArr.NumAerA
and DAeroportArr.TypeAerA = 'Nationale';

/*Réponse 7*/
/*Création d'un index bitmap de jointure entre FTraffic et DAeroportArr, basé sur l’attribut ‘TypeAer’.*/

Create Bitmap Index Intex_Bitmap on FTraffic(DAeroportArr.TypeAerA)
FROM FTraffic , DAeroportArr  
Where (FTraffic.NumAerA=DAeroportArr.NumAerA) TABLESPACE Master2_tbs;


/*Réponse 8*/
/*Réexécution de la requête R2*/

alter system flush shared_pool;
alter system flush buffer_cache;

Select count(FTraffic.NbVolRetard) as NombreVolRetard
From FTraffic ,DAeroportArr
where FTraffic.NumAerA = DAeroportArr.NumAerA
and DAeroportArr.TypeAerA = 'Nationale';


/*Réponse 9*/
/*Création de la table FTraffic2 identique à FTraffic, en la partitionnant en fonction des code de compagnie*/
CREATE TABLE FTraffic2 (
    NumAerD INTEGER,
    NumAerA INTEGER,
    CodeComp INTEGER,
    CodeModele INTEGER,
    CodeTemps INTEGER,
    NbVol INTEGER,
    NbVolRetard INTEGER,
	CONSTRAINT FK_1 FOREIGN KEY (NumAerD)REFERENCES DAeroportDep(NumAerD),
    CONSTRAINT FK_2 FOREIGN KEY (NumAerA) REFERENCES DAeroportArr(NumAerA),
    CONSTRAINT FK_3 FOREIGN KEY (CodeComp)REFERENCES DCompagnie(CodeComp),
	CONSTRAINT FK_4 FOREIGN KEY (CodeModele)REFERENCES DModele(CodeModele),
    CONSTRAINT FK_5 FOREIGN KEY (CodeTemps)REFERENCES DTemps(CodeTemps),
    PRIMARY KEY (NumAerD, NumAerA,CodeComp, CodeModele,CodeTemps)
	)
	PARTITION BY range(CodeComp)
	(
	partition P1 values LESS THAN (50),
	partition P2 values LESS THAN (100),
	partition P3 values LESS THAN (150),
	partition P4 values LESS THAN (MAXVALUE)
	);

/*Réponse 10*/
/*Remplissage de la table FTraffic2 les avec les mêmes instances que FTraffic.*/

begin
for i in
( SELECT FTraffic.NumAerD, FTraffic.NumAerA, FTraffic.CodeComp, 
FTraffic.CodeModele,FTraffic.CodeTemps,FTraffic.NbVol,FTraffic.NbVolRetard
From FTraffic
)
loop
insert into FTraffic2 values(i.NumAerD,i.NumAerA,i.CodeComp,i.CodeModele,i.CodeTemps,i.NbVol,i.NbVolRetard);
end loop;
commit ;
end ;
/
/*Réponse 11*/
/* Ecriture de la requête R3 qui donne le le Nombre de vol en retard de la compagnie N°124 en utilisant la table FTraffic.*/

alter system flush shared_pool;
alter system flush buffer_cache;

Select sum(FTraffic.NbVolRetard) as NombreVolRetard
from FTraffic
where FTraffic.CodeComp = 124;

/*Réponse 12*/
/*Modification de la requête R3 pour l'utilisation de la table FTraffic2.*/

alter system flush shared_pool;
alter system flush buffer_cache;

Select sum(FTraffic2.NbVolRetard) as NombreVolRetard
from FTraffic2
where FTraffic2.CodeComp = 124;