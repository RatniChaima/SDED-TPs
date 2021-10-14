/* RATNI Chaima immatriculé 201500010292 G3 M2 IL*/


/*++++++++++++++++++++++++++++++++++++++++TP3++++++++++++++++++++++++++++++++++++++++*/

/*Reponse 1*/
--Activer le timing de oracle
set timing on

--Activer le autotrace de oracle
set autotrace on

/*Reponse 2*/
--Requête R1 pour obtenir la liste des vols entre l’Algérie et la Suède
-- Mise a jour dans la table Pays
update Pays set NomPays='Algerie' where CodePays=1;
update Pays set NomPays='Suede' where CodePays=2;

--Vider les buffers
alter system flush shared_pool;
alter system flush buffer_cache;

select NumVol, DateVol 
from Vol vol, Aeroport a1, Aeroport a2, Ville v1, Ville v2, Pays p1, Pays p2
where  
		a1.CodeVille=v1.CodeVille and a2.CodeVille=v2.CodeVille 
	and v1.CodePays=p1.CodePays and v2.CodePays=p2.CodePays 
	and vol.NumAerDepart=a1.NumAer and vol.NumAerDepart=a2.NumAer
	and vol.NumAerArr=a1.NumAer and vol.NumAerArr=a2.NumAer
	and p1.codePays=1 and v2.CodePays=2 
;

/*Reponse 3*/
--Examination du temps et du plan d’exécution.

/*Reponse 4*/
--Création de la vue matérialisée VM1 contenant une jointure entre les tables vol, aéroport, ville et pays.
CREATE MATERIALIZED VIEW VM1
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS select NumVol, DateVol 
from Vol vol, Aeroport a1, Aeroport a2, Ville v1, Ville v2, Pays p1, Pays p2
where  
		a1.CodeVille=v1.CodeVille and a2.CodeVille=v2.CodeVille 
	and v1.CodePays=p1.CodePays and v2.CodePays=p2.CodePays 
	and vol.NumAerDepart=a1.NumAer and vol.NumAerDepart=a2.NumAer
	and vol.NumAerArr=a1.NumAer and vol.NumAerArr=a2.NumAer
	and p1.codePays=1 and v2.CodePays=2 
;	
	
/*Reponse 5*/
--Vider les buffers
alter system flush shared_pool;
alter system flush buffer_cache;
--Ré exécuter la requête R1.
select NumVol, DateVol 
from Vol vol, Aeroport a1, Aeroport a2, Ville v1, Ville v2, Pays p1, Pays p2
where  
		a1.CodeVille=v1.CodeVille and a2.CodeVille=v2.CodeVille 
	and v1.CodePays=p1.CodePays and v2.CodePays=p2.CodePays 
	and vol.NumAerDepart=a1.NumAer and vol.NumAerDepart=a2.NumAer
	and vol.NumAerArr=a1.NumAer and vol.NumAerArr=a2.NumAer
	and p1.codePays=1 and v2.CodePays=2 
;
					
/*Reponse 6*/
--Ecrire une requête R2 pour obtenir le nombre de vol par Année (Année, NBVols)
SELECT TRUNC(DateVol,'YYYY') as Annee ,count(NumVol) as NBVols
FROM Vol
group by TRUNC(DateVol,'YYYY')
order by TRUNC(DateVol,'YYYY');

/*Reponse 7*/
--Examination du temps et du plan d’exécution.

/*Reponse 8*/
--Création de la vue matérialisée VM2 (Année, NbVols) 
CREATE MATERIALIZED VIEW VM2
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
	AS SELECT TRUNC(DateVol,'YYYY') as Annee ,count(NumVol) as NBVols
	FROM Vol
	group by TRUNC(DateVol,'YYYY')
	order by TRUNC(DateVol,'YYYY')
;
/*Réponse 9*/
--Vider les buffers
alter system flush shared_pool;
alter system flush buffer_cache;
--Ré exécuter la requête R2.
SELECT TRUNC(DateVol,'YYYY') as Annee ,count(NumVol) as NBVols
FROM Vol
group by TRUNC(DateVol,'YYYY')
order by TRUNC(DateVol,'YYYY');

/*Reponse 10*/
--Augmenter le nombre d’instances de vol à 1200000
DECLARE
TV number; 
R number; 
D number; 
dt date; 
DP number; 
AR number ;
P number; 
C number;
A number ;
BEGIN
FOR i IN 865301.. 1200000 LOOP
Select floor(dbms_random.value(0, 1.9)) into TV from dual;
Select floor(dbms_random.value(0, 1.9)) into R from dual;
Select floor(dbms_random.value(30, 12000.9)) into D from dual;
SELECT TO_DATE( TRUNC( DBMS_RANDOM.VALUE(TO_CHAR(DATE '2016-01-01','J')
,TO_CHAR(DATE '2020-12-31','J') )
),'J'
) into dt FROM DUAL;
Select floor(dbms_random.value(1, 17600.9)) into DP from dual;
Select floor(dbms_random.value(1, 17600.9)) into AR from dual;
Select floor(dbms_random.value(1, 9314.9) ) into P from dual;
Select floor(dbms_random.value(1, 200.9) ) into C from dual;
Select floor(dbms_random.value(1, 8600.9) ) into A from dual;
INSERT INTO Vol VALUES (i, TV, R, D, dt,DP,AR,P,C,A);
END LOOP;
COMMIT;
END;
/

--Sans vue VM2
ALTER MATERIALIZED VIEW VM2 DISABLE QUERY REWRITE;
--retester la requête R2
SELECT TRUNC(DateVol,'YYYY') as Annee ,count(NumVol) as NBVols
FROM Vol
group by TRUNC(DateVol,'YYYY')
order by TRUNC(DateVol,'YYYY');
--Avec la vue VM2
ALTER MATERIALIZED VIEW VM2 Enable QUERY REWRITE;
--rafraichir la vue
Execute DBMS_MVIEW.Refresh(‘VM2’) ;
--retester la requête R2
SELECT TRUNC(DateVol,'YYYY') as Annee ,count(NumVol) as NBVols
FROM Vol
group by TRUNC(DateVol,'YYYY')
order by TRUNC(DateVol,'YYYY');


--Augmenter le nombre d’instances de vol à 2000000
DECLARE
TV number; 
R number; 
D number; 
dt date; 
DP number; 
AR number ;
P number; 
C number;
A number ;
BEGIN
FOR i IN 1200001..2000000 LOOP
Select floor(dbms_random.value(0, 1.9)) into TV from dual;
Select floor(dbms_random.value(0, 1.9)) into R from dual;
Select floor(dbms_random.value(30, 12000.9)) into D from dual;
SELECT TO_DATE( TRUNC( DBMS_RANDOM.VALUE(TO_CHAR(DATE '2016-01-01','J')
,TO_CHAR(DATE '2020-12-31','J') )
),'J'
) into dt FROM DUAL;
Select floor(dbms_random.value(1, 17600.9)) into DP from dual;
Select floor(dbms_random.value(1, 17600.9)) into AR from dual;
Select floor(dbms_random.value(1, 9314.9) ) into P from dual;
Select floor(dbms_random.value(1, 200.9) ) into C from dual;
Select floor(dbms_random.value(1, 8600.9) ) into A from dual;
INSERT INTO Vol VALUES (i, TV, R, D, dt,DP,AR,P,C,A);
END LOOP;
COMMIT;
END;
/
			

--Sans vue VM2
ALTER MATERIALIZED VIEW VM2 DISABLE QUERY REWRITE;
--retester la requête R2
SELECT TRUNC(DateVol,'YYYY') as Annee ,count(NumVol) as NBVols
FROM Vol
group by TRUNC(DateVol,'YYYY')
order by TRUNC(DateVol,'YYYY');
--Avec la vue VM2
ALTER MATERIALIZED VIEW VM2 Enable QUERY REWRITE;
--rafraichir la vue
Execute DBMS_MVIEW.Refresh(‘VM2’) ;
--retester la requête R2
SELECT TRUNC(DateVol,'YYYY') as Annee ,count(NumVol) as NBVols
FROM Vol
group by TRUNC(DateVol,'YYYY')
order by TRUNC(DateVol,'YYYY');


/*++++++++++++++++++++++++++++++++++++++++TP4++++++++++++++++++++++++++++++++++++++++*/

/* Création des tablespaces */
CREATE TABLESPACE Master2_tbs 
    DATAFILE 'D:\Master2.dat'
    SIZE 100M AUTOEXTEND ON ONLINE;

CREATE TEMPORARY TABLESPACE TempTBS2 
    TEMPFILE 'D:\TempTBSFile2.dat' 
    SIZE 100M AUTOEXTEND ON;
	

/* Création d'un compte Utilisateur 'Master2' */
CREATE USER Master2 
    IDENTIFIED BY psw2 
    DEFAULT TABLESPACE Master2_tbs 
    TEMPORARY TABLESPACE TempTBS2;

/*Attribution de tous les privilèges à l'utilisateur 'Master2' */
GRANT ALL PRIVILEGES TO Master2;

/* Se connecter en tant que l'utilisateur 'Master2' */
Disconnect;
Connect Master2/psw2;

GRANT ALL PRIVILEGES ON Master2 To Master;

/*Création de la table DTemps*/
CREATE TABLE DTemps (
    CodeTemps integer,
    Jour VARCHAR2(15),
    LibJour VARCHAR2(15),
    Mois VARCHAR2(15),
    LibMois VARCHAR2(15),
    Annee VARCHAR2(15),
	primary key (CodeTemps)
);

/*Création de la table DCompagnie*/
CREATE TABLE DCompagnie(CodeComp integer,
NomComp varchar2(20), 
CodeTypeComp integer , 
TypeComp varchar2(20), 
primary key (CodeComp)
);

/*Création de la table DModele*/
CREATE TABLE DModele (CodeModele integer,  
LibModele varchar2(50), 
CodeCons integer, 
NomConst varchar2(50),
primary key (CodeModele)
);


/*Création de la table DAeroportDep*/
CREATE TABLE DAeroportDep (NumAerD integer,
NomAerD varchar2(20),
CodeVille  integer,
NomVille varchar(50),
CodePays integer,
NomPays varchar(50),
CodeTypeAerD integer, 
TypeAerD varchar2(20),
primary key (NumAerD)
);

/*Création de la table DAeroportArr*/
CREATE TABLE DAeroportArr(NumAerA integer,
NomAerA varchar(20),  
CodeVille  integer,
NomVille varchar(50),
CodePays integer,
NomPays varchar(50),
CodeTypeAerA integer , 
TypeAerA varchar2(20),
primary key (NumAerA)
);


/*Création de la table FTraffic*/
CREATE TABLE FTraffic (
    NumAerD INTEGER,
    NumAerA INTEGER,
    CodeComp INTEGER,
    CodeModele INTEGER,
    CodeTemps INTEGER,
    NbVol INTEGER,
    NbVolRetard INTEGER,
    CONSTRAINT FK_O1 FOREIGN KEY (NumAerD)REFERENCES DAeroportDep(NumAerD),
    CONSTRAINT FK_O2 FOREIGN KEY (NumAerA) REFERENCES DAeroportArr(NumAerA),
    CONSTRAINT FK_O3 FOREIGN KEY (CodeComp)REFERENCES DCompagnie(CodeComp),
	CONSTRAINT FK_O4 FOREIGN KEY (CodeModele)REFERENCES DModele(CodeModele),
    CONSTRAINT FK_O5 FOREIGN KEY (CodeTemps)REFERENCES DTemps(CodeTemps),
    PRIMARY KEY (NumAerD, NumAerA,CodeComp, CodeModele,CodeTemps)
);

/* --------Remplissage des tables------- */

/*Création d'une séquence pour l’utiliser dans le remplissage de la table DTemps*/
CREATE SEQUENCE seq MINVALUE 1 MAXVALUE 1000000 START WITH 1 INCREMENT BY 1;

/* Remplissage de la table DTemps*/
begin
 for i in (SELECT distinct TO_CHAR(DateVol,'DD/MM/YYYY') as Jour, TO_CHAR(DateVol,'DAY') as LibJour, 
            TO_CHAR(DateVol,'MM/YYYY') as Mois, TO_CHAR(DateVol,'MONTH') as LibMois, 
            TO_CHAR(DateVol,'YYYY') as Annee 
            FROM Master.Vol)
    loop
        insert into DTemps values (seq.NEXTVAL, i.Jour, i.LibJour, i.Mois, i.LibMois, i.Annee);
    end loop;
commit;
end;
/

/* Remplissage de la table DCompagnie*/

begin
	for i in ( SELECT c.CodeComp, c.NomComp, tc.CodeTypeComp, tc.TypeComp
	FROM Master.Compagnie c, Master.TypeCompagnie tc
	WHERE c.CodeTypeComp=tc.CodeTypeComp) 
	loop
		insert into DCompagnie values(i.CodeComp, i.NomComp, i.CodeTypeComp, i.TypeComp);
	end loop;
	commit;
end;
/

/*Remplissage de la table DModele*/
begin
for i in
( SELECT m.CodeMod, m.LibMod, cont.CodeConst, cont.NomConst
FROM Master.Modele m, Master.Constructeur cont
WHERE m.CodeConst=cont.CodeConst) 
loop
insert into DModele values(i.CodeMod, i.LibMod,i.CodeConst,i.NomConst);
end loop;
commit ;
end ;
/

/*Remplissage de la table DAeroportDep*/
begin
for i in
( SELECT A.NumAer, A.NomAer, V.CodeVille, V.NomVille, P.CodePays, P.NomPays, T.CodeTypeAer, T.TypeAer
FROM Master.Aeroport A, Master.Ville V, Master.Pays P, Master.TypeAeroport T
WHERE A.CodeVille=V.CodeVille 
and V.CodePays=P.CodePays and A.CodeTypeAer=T.CodeTypeAer
)
loop
insert into DAeroportDep values(i.NumAer, i.NomAer, i.CodeVille, i.NomVille, i.CodePays, i.NomPays, i.CodeTypeAer, i.TypeAer);
end loop;
commit ;
end ;
/

/*Remplissage de la table DAeroportArr*/
begin
for i in
( SELECT A.NumAer, A.NomAer, V.CodeVille, V.NomVille, P.CodePays, P.NomPays, T.CodeTypeAer, T.TypeAer
FROM Master.Aeroport A, Master.Ville V, Master.Pays P, Master.TypeAeroport T
WHERE (A.CodeVille=V.CodeVille)
and (V.CodePays=P.CodePays) 
and (A.CodeTypeAer=T.CodeTypeAer)
)
loop
insert into DAeroportArr values(i.NumAer, i.NomAer, i.CodeVille, i.NomVille, i.CodePays, i.NomPays, i.CodeTypeAer, i.TypeAer);
end loop;
commit ;
end ;
/

/*Remplissage de la table FTraffic*/
begin
for i in
( SELECT d.NumAerD, a.NumAerA, c.CodeComp, m.CodeModele,t.CodeTemps,count(*) as Nombrevols, sum(CAST(v.Retard AS int)) as NombreRetard
FROM DAeroportDep d, DAeroportArr a, DModele m, DCompagnie c, Master.Avion av, Master.Vol v, DTemps t
WHERE (d.NumAerD=v.NumAerDepart)
and (a.NumAerA=v.NumAerArr)
and (c.CodeComp=v.CodeComp)
and (v.NumAvion=av.NumAvion )
and (t.Jour=TO_CHAR(v.DateVol,'DD/MM/YYYY'))
and(av.CodeMod=m.CodeModele) 
group by d.NumAerD, a.NumAerA, c.CodeComp, m.CodeModele,t.CodeTemps
)
loop
insert into FTraffic values(i.NumAerD,i.NumAerA,i.CodeComp,i.CodeModele,i.CodeTemps,i.Nombrevols,i.NombreRetard);
end loop;
commit ;
end ;
/