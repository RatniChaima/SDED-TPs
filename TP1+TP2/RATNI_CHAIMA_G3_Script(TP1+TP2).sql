/*
  Script TP 1 + TP 2
  Réalisé par: RATNI Chaima
  Master 2 IL - Groupe 3

*/
/*///////////////////////////////// TP1 //////////////////////////////////////*/


/* Réponse 1 */

/* Création des tablespaces */
CREATE TABLESPACE Master_tbs 
    DATAFILE 'D:\Master.dat'
    SIZE 100M AUTOEXTEND ON ONLINE;

CREATE TEMPORARY TABLESPACE TempTBS 
    TEMPFILE 'D:\TempTBSFile.dat' 
    SIZE 100M AUTOEXTEND ON;
	
	DROP TABLESPACE TempTBS 
   INCLUDING CONTENTS AND DATAFILES;

/* Création d'un compte Utilisateur 'Master' */
CREATE USER Master 
    IDENTIFIED BY psw 
    DEFAULT TABLESPACE Master_tbs 
    TEMPORARY TABLESPACE TempTBS;

/*Attribution de tous les privilèges à l'utilisateur 'Master' */
GRANT ALL PRIVILEGES TO Master;

/* Se connecter en tant que l'utilisateur 'Master' */
Disconnect;
Connect Master/psw;

/* Réponse 3 */

/*Création de la table TypeAeroport*/

CREATE TABLE TypeAeroport(
    CodeTypeAer NUMBER ,
    TypeAer VARCHAR2(50) ,
    PRIMARY KEY(CodeTypeAer),	
	CONSTRAINT CHK_TypeAer CHECK (TypeAer='Nationale' OR TypeAer='Internationale'))
;

/*Création de la table Pays*/
CREATE TABLE Pays(
    CodePays NUMBER ,
    NomPays VARCHAR2(50) ,
    PRIMARY KEY(CodePays))
;


/*Création de la table Ville*/
CREATE TABLE Ville(
    CodeVille NUMBER ,
    NomVille VARCHAR2(50) ,
    CodePays NUMBER ,
    PRIMARY KEY(CodeVille),
	CONSTRAINT fk_CodePays FOREIGN KEY (CodePays) REFERENCES Pays(CodePays))
;


/*Création de la table Aeroport*/
CREATE TABLE Aeroport(
    NumAer NUMBER ,
    NomAer VARCHAR2(50),
	CodeVille NUMBER ,
	CodeTypeAer NUMBER ,
    PRIMARY KEY(NumAer),
	CONSTRAINT fk_CodeVille FOREIGN KEY (CodeVille) REFERENCES Ville(CodeVille),
	CONSTRAINT fk_CodeTypeAer FOREIGN KEY (CodeTypeAer) REFERENCES TypeAeroport(CodeTypeAer))
;


/*Création de la table Pilote*/
CREATE TABLE Pilote(
    CodeP NUMBER ,
    NomP VARCHAR2(50) ,
    PRIMARY KEY(CodeP))
;

/*Création de la table TypeCompagnie*/
CREATE TABLE TypeCompagnie(
    CodeTypeComp NUMBER ,
    TypeComp VARCHAR2(50) ,
    PRIMARY KEY(CodeTypeComp),
	CONSTRAINT CHK_TypeComp CHECK (TypeComp='Etatique' OR TypeComp='Privee'))
;

/*Création de la table Compagnie*/
CREATE TABLE Compagnie(
    CodeComp NUMBER ,
    NomComp VARCHAR2(50) ,
	CodePays NUMBER ,
	CodeTypeComp NUMBER ,
    PRIMARY KEY(CodeComp),
	CONSTRAINT fk_CodePays_Comp FOREIGN KEY (CodePays) REFERENCES Pays(CodePays),
	CONSTRAINT fk_CodeTypeComp FOREIGN KEY (CodeTypeComp) REFERENCES TypeCompagnie(CodeTypeComp))
;

/*Création de la table Constructeur*/
CREATE TABLE Constructeur(
    CodeConst NUMBER ,
    NomConst VARCHAR2(50) ,
    PRIMARY KEY(CodeConst))
;

/*Création de la table Modele*/
CREATE TABLE Modele(
    CodeMod NUMBER ,
    LibMod VARCHAR2(50) ,
	CodeConst NUMBER ,
    PRIMARY KEY(CodeMod),
	CONSTRAINT fk_CodeConst FOREIGN KEY (CodeConst) REFERENCES Constructeur(CodeConst))
;

/*Création de la table Avion*/
CREATE TABLE Avion(
    NumAvion NUMBER ,
	CodeMod NUMBER ,
    PRIMARY KEY(NumAvion),
	CONSTRAINT fk_CodeMod FOREIGN KEY (CodeMod) REFERENCES Modele(CodeMod))
;

/*Création de la table Vol*/
CREATE TABLE Vol(
    NumVol NUMBER ,
    TypeVol NUMBER ,
    Retard NUMBER,
	Duree_theorique NUMBER,
	DateVol date,
	NumAerDepart NUMBER ,
	NumAerArr NUMBER ,
	CodeP NUMBER ,
	CodeComp NUMBER ,
	NumAvion NUMBER ,
    PRIMARY KEY(NumVol),
	CONSTRAINT FK_NumDepart FOREIGN KEY (NumAerDepart) REFERENCES Aeroport (NumAer),
	CONSTRAINT FK_NumArr FOREIGN KEY (NumAerArr) REFERENCES Aeroport (NumAer),
	CONSTRAINT fk_CodeP FOREIGN KEY (CodeP) REFERENCES Pilote(CodeP),
	CONSTRAINT fk_CodeComp FOREIGN KEY (CodeComp) REFERENCES Compagnie(CodeComp),
	CONSTRAINT fk_NumAvion FOREIGN KEY (NumAvion) REFERENCES Avion(NumAvion),
	CHECK (TypeVol=0 or TypeVol=1),
	CHECK (Retard=0 or Retard=1))
;


/*///////////////////////////////// TP2 //////////////////////////////////////*/

/*-------Remplissage des tables-----*/

/*Remplissage table Pays*/
DECLARE
I number;
v char(10);
begin
for i in 1..194 loop
Select dbms_random.string('U',8) into v from dual;
insert into Pays values(I,v);
end loop;
commit;
end;
/

/*Remplissage table Ville*/
DECLARE
w number;
v char(45);
I number;
begin
for i in 1..12000 loop
Select dbms_random.string('U', 8) into v from dual;
Select floor(dbms_random.value(1, 194.9)) into w from dual;
insert into Ville values(I,v,w);
end loop;
commit;
end;
/

/*Remplissage table TypeAeroport-*/
begin
INSERT INTO TypeAeroport values(1,'Internationale');
INSERT INTO TypeAeroport values(2,'Nationale');
commit;
end;
/

/*Remplissage table TypeCompagnie*/
begin
INSERT INTO TypeCompagnie values(1, 'Etatique');
INSERT INTO TypeCompagnie values(2, 'Privee');
commit;
end;
/

/*Remplissage table Pilote*/
DECLARE
I number;
n char(10);
begin
for i in 1..9314 loop
Select dbms_random.string('U',8) into n from dual;
insert into Pilote values(I,n);
end loop;
commit;
end;
/

/*Remplissage table Constructeur*/
DECLARE 
I number;
c char(10);
begin
for i in 1..5 loop
Select dbms_random.string('U',8) into c from dual;
insert into Constructeur values(I,c);
end loop;
commit;
end;
/

/*Remplissage table Modele*/
DECLARE 
I number;
l char(10);
c char(10);
begin
for i in 1..120 loop
Select dbms_random.string('U',8) into l from dual;
Select floor(dbms_random.value(1, 5.9)) into c from dual;
insert into Modele values(I,l,c);
end loop;
commit;
end;
/ 

/*Remplissage table Avion*/
DECLARE 
I number;
m number;
begin
for i in 1..8600 loop
Select floor(dbms_random.value(1, 120.9)) into m from dual;
insert into Avion values(I,m);
end loop;
commit;
end;
/

/*Remplissage table Aeroport*/
DECLARE 
I number;
a char(10);
v number;
t number;
begin
for i in 1..17600 loop
Select dbms_random.string('U',8) into a from dual;
Select floor(dbms_random.value(1, 12000.9)) into v from dual;
Select floor(dbms_random.value(1, 2.9)) into t from dual;
insert into Aeroport values(I,a,v,t);
end loop;
commit;
end;
/

/*Remplissage table Compagnie*/
DECLARE 
I number;
n char(10);
p number;
c number;
begin
for i in 1..200 loop
Select dbms_random.string('U',8) into n from dual;
Select floor(dbms_random.value(1, 194.9)) into p from dual;
Select floor(dbms_random.value(1, 2.9)) into c from dual;
insert into Compagnie values(I,n,p,c);
end loop;
commit;
end;
/

/*Remplissage table Vol*/
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
FOR i IN 1.. 865300 LOOP
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
INSERT INTO vol VALUES (i, TV, R, D, dt,DP,AR,P,C,A);
END LOOP;
COMMIT;
END;
/