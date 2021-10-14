
/* RATNI CHAIMA 
immatriculé 201500010292 
G3 M2 IL */

/*---------------------------------------- TP5 ----------------------------------------*/
/*Réponse 1*/
/*Activation des options autotrace et timing de oracle*/
set timing on;
set autotrace on explain;

/*Vider les buffers*/
alter system flush shared_pool;
alter system flush buffer_cache;

/* ajouter le pays Portugal*/
update Pays set NomPays='Portugal' where CodePays=77;


-- La requête R1 qui donne le nombre de vols en retard en provenance du Portugal
select sum(T.NbVolRetard) 
from FTraffic T, DAeroportDep Adep 
where T.NumAerD=Adep.NumAerD 
and Adep.NomPays='Portugal';

/*Réponse 2*/

-- La vue matérialisée VMWilaya (CodePays,NomPays, NBVolRetard), qui donne le nombre de vols en retard en provenance de chaque pays.
CREATE MATERIALIZED VIEW VMWilaya
    BUILD IMMEDIATE 
    REFRESH COMPLETE ON DEMAND
    ENABLE QUERY REWRITE
    AS select Ad.CodePays, Ad.NomPays, 
                sum(FT.NbVolRetard) as NbRetard
       from FTraffic FT, DAeroportDep Ad
      where FT.NumAerD=Ad.NumAerD
      group by Ad.CodePays, Ad.NomPays
      order by Ad.CodePays;

/*Réponse 3*/
-- Vider les buffers
alter system flush shared_pool;
alter system flush buffer_cache;

/*Réponse 4*/
-- Création d’une vue matérialisée VMNBVolMensuel(Mois, NBVol), afin de stocker les nombres de vols mensuels

CREATE MATERIALIZED VIEW VMNBVolMensuel
    BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND
    enable query rewrite
    AS select DTemps.Mois, 
            sum(FTraffic.NbVol) as MVMensuel
       from DTemps , FTraffic 
      where FTraffic.CodeTemps = DTemps.CodeTemps
      group by DTemps.Mois
      order by DTemps.Mois;
	  
	  
/*Réponse 5*/
/*Vider les buffers*/
alter system flush shared_pool;
alter system flush buffer_cache;

--Ecrire et exécuter une requête R2 qui donne les nombres de vols annuels (sans tenir compte de VMNBVolMensuel )
select DTemps.Annee, sum(FTraffic.NbVol) as NBVolMensuel
       from DTemps , FTraffic 
      where FTraffic.CodeTemps = DTemps.CodeTemps
      group by DTemps.Annee
      order by DTemps.Annee;

/*Réponse 6*/
/*Création des méta données de toutes les dimensions*/

-- la dimesion DIMTemps
create dimension DIMTemps 
level NTemps1 is (DTemps.CodeTemps)
level NTemps2 is (DTemps.Mois)
level NTemps3 is (DTemps.Annee)
hierarchy HTemps1 (NTemps1 child of NTemps2 child of NTemps3)
attribute NTemps1 determines (DTemps.Jour, DTemps.LibJour)
attribute NTemps2 determines (DTemps.LibMois)
;

-- la dimesion DIMModele
create dimension DIMModele
LEVEL NModele1 IS DModele.CodeModele
LEVEL NModele2 IS DModele.CodeCons
HIERARCHY HModele1(NModele1 child of NModele2)
ATTRIBUTE NModele1 DETERMINES DModele.LibModele
ATTRIBUTE NModele2 DETERMINES DModele.NomConst
;

-- la dimesion DIMCompagnie
create dimension DIMCompagnie
LEVEL NACompagnie1 IS DCompagnie.CodeComp
LEVEL NACompagnie2 IS DCompagnie.CodeTypeComp
ATTRIBUTE NACompagnie1 DETERMINES DCompagnie.NomComp
ATTRIBUTE NACompagnie2 DETERMINES DCompagnie.TypeComp
;

-- la dimesion DIMAeroportDep
create dimension DIMAeroportDep
LEVEL  NAeroportDep1 IS  DAeroportDep.NumAerD
LEVEL  NAeroportDep2 IS  DAeroportDep.CodeVille
LEVEL  NAeroportDep3 IS DAeroportDep.CodePays
LEVEL  NAeroportDep4 IS DAeroportDep.CodeTypeAerD
HIERARCHY HAeroportDep1( NAeroportDep1 child of  NAeroportDep2 CHILD OF NAeroportDep3 CHILD OF NAeroportDep4)
ATTRIBUTE  NAeroportDep1 DETERMINES  DAeroportDep.NomAerD
ATTRIBUTE  NAeroportDep2 DETERMINES  DAeroportDep.NomVille
ATTRIBUTE  NAeroportDep3 DETERMINES  DAeroportDep.NomPays
ATTRIBUTE  NAeroportDep4 DETERMINES  DAeroportDep.TypeAerD
;

-- la dimesion DIMAeroportArr
create dimension DIMAeroportArr
LEVEL  NAeroportArr1 IS  DAeroportArr.NumAerA
LEVEL  NAeroportArr2 IS  DAeroportArr.CodeVille
LEVEL  NAeroportArr3 IS DAeroportArr.CodePays
LEVEL  NAeroportArr4 IS DAeroportArr.CodeTypeAerA
HIERARCHY HAeroportArr1( NAeroportArr1 child of  NAeroportArr2 CHILD OF NAeroportArr3 CHILD OF NAeroportArr4)
ATTRIBUTE  NAeroportArr1 DETERMINES  DAeroportArr.NomAerA
ATTRIBUTE  NAeroportArr2 DETERMINES  DAeroportArr.NomVille
ATTRIBUTE  NAeroportArr3 DETERMINES  DAeroportArr.NomPays
ATTRIBUTE  NAeroportArr4 DETERMINES  DAeroportArr.TypeAerA
;


/*Réponse 7*/
-- Modification de la session afin de permettre l’exploitation des dimensions dans l’amélioration des temps d’exécutions
Alter session set query_rewrite_integrity = trusted;

/*Réponse 8*/
-- Vider les buffers
alter system flush shared_pool;
alter system flush buffer_cache;

-- Réexécution de la requête R2
select DTemps.Annee, sum(FTraffic.NbVol) as NBVolMensuel
       from DTemps , FTraffic 
      where FTraffic.CodeTemps = DTemps.CodeTemps
      group by DTemps.Annee
      order by DTemps.Annee;
	  
/*Réponse 9*/
-- Création d’une vue matérialisée VMNBVolVille(CodeVille, NomVille, MV) qui stocke le nombre de vol en provenance de chaque ville
CREATE MATERIALIZED VIEW VMNBVolVille
    BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND
    enable query rewrite
    AS select DAeroportDep.CodeVille, DAeroportDep.NomVille, sum(FTraffic.NbVol) as mv
       from DAeroportDep , FTraffic 
      where FTraffic.NumAerD = DAeroportDep.NumAerD
      group by DAeroportDep.CodeVille, DAeroportDep.NomVille
      order by DAeroportDep.CodeVille;

/*Réponse 10*/
-- Vider les buffers
alter system flush shared_pool;
alter system flush buffer_cache;

-- Ecrire une requête R3 qui donne lle nombre de vol en provenance de chaque pays (sans tenir compte de VMNBVolVille)
Select DAeroportDep.CodePays,Sum(FTraffic.NbVol) AS NBVol
from FTraffic,DAeroportDep
where FTraffic.NumAerD=DAeroportDep.NumAerD
group by DAeroportDep.CodePays
order by DAeroportDep.CodePays
;

/*Réponse 11*/
-- Examination du temps et du plan d’exécution

/*Réponse 12*/
-- Suppression des méta données de la dimension DAeroportDep
drop dimension DAeroportDep;


-- Vider les buffers
alter system flush shared_pool;
alter system flush buffer_cache;

-- Réexécution de la requête R3
Select DAeroportDep.CodePays,Sum(FTraffic.NbVol) AS NBVol
from FTraffic,DAeroportDep
where FTraffic.NumAerD=DAeroportDep.NumAerD
group by DAeroportDep.CodePays
order by DAeroportDep.CodePays
;
	  
/*---------------------------------------- TP6 ----------------------------------------*/


/*Activation l’option timing de oracle*/ 
set timing on;

/*Réponse 1*/
-- Quels sont les nombres de vols annuels par Pays de départ, pour chaque type de compagnie.

Select DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp, 
        sum(FTraffic.NbVol) as NbVolAnnuel 
from FTraffic , DCompagnie , DTemps, DAeroportDep 
where FTraffic.CodeComp = DCompagnie.CodeComp
      and FTraffic.CodeTemps = DTemps.CodeTemps
      and FTraffic.NumAerD=DAeroportDep.NumAerD
group by DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp
order by DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp
;

/*Réponse 2*/
-- Introduction d es sous totaux (sur la requête 1) avec la clause rollup by

Select DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp, 
        sum(FTraffic.NbVol) as NbVolAnnuel 
from FTraffic , DCompagnie , DTemps, DAeroportDep 
where FTraffic.CodeComp = DCompagnie.CodeComp
      and FTraffic.CodeTemps = DTemps.CodeTemps
      and FTraffic.NumAerD=DAeroportDep.NumAerD
group by rollup (DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp)
order by DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp
;

/*Réponse 3*/
-- Introduire les sous totaux (sur la requête 1) avec la clause cube by

Select DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp, 
        sum(FTraffic.NbVol) as NbVolAnnuel 
from FTraffic , DCompagnie , DTemps, DAeroportDep 
where FTraffic.CodeComp = DCompagnie.CodeComp
      and FTraffic.CodeTemps = DTemps.CodeTemps
      and FTraffic.NumAerD=DAeroportDep.NumAerD
group by cube (DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp)
order by DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp
;

/*Réponse 4*/
-- Introduire la fonction grouping pour chaque dimension (sur la requête 2)

Select DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp, 
        sum(FTraffic.NbVol) as NbVolAnnuel, 
grouping (DTemps.Annee) as Annee, grouping (DAeroportDep.NomPays) as NomAerDep, 
        grouping (DCompagnie.CodeTypeComp) as TypeComp
from FTraffic , DCompagnie , DTemps, DAeroportDep 
where FTraffic.CodeComp = DCompagnie.CodeComp
      and FTraffic.CodeTemps = DTemps.CodeTemps
      and FTraffic.NumAerD=DAeroportDep.NumAerD
group by rollup (DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp)
order by DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp
;


/*Réponse 5*/
-- Remplacer la fonction grouping par la fonction grouping_id

Select DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp, 
        sum(FTraffic.NbVol) as NbVolAnnuel, 
grouping_id (DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp) as GID
from FTraffic , DCompagnie , DTemps, DAeroportDep 
where FTraffic.CodeComp = DCompagnie.CodeComp
      and FTraffic.CodeTemps = DTemps.CodeTemps
      and FTraffic.NumAerD=DAeroportDep.NumAerD
group by rollup (DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp)
order by DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp
;

/*Réponse 6*/
-- Amélioration de la lisibilité de la requête précédente en utilisant la fonction decode

Select decode(grouping (DTemps.Annee),1,'An',DTemps.Annee)as an,
decode(grouping (DAeroportDep.NomPays),1,'NP',DAeroportDep.NomPays)as np,
decode(grouping (DCompagnie.CodeTypeComp),1,'TC',DCompagnie.CodeTypeComp) as ctc,
sum(FTraffic.NbVol) as NbVolAnnuel
from FTraffic , DCompagnie , DTemps, DAeroportDep 
where FTraffic.CodeComp = DCompagnie.CodeComp
      and FTraffic.CodeTemps = DTemps.CodeTemps
      and FTraffic.NumAerD=DAeroportDep.NumAerD
group by rollup (DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp)
order by DTemps.Annee, DAeroportDep.NomPays, DCompagnie.CodeTypeComp
;

/*Réponse 7*/
--Donner le classement dense et non dense des compagnies dans chaque année selon le nombre de vol en retard.

/*Classement non-dense*/
Select DCompagnie.CodeComp,DTemps.Annee,sum(FTraffic.NbVolRetard) as Nbr,
rank() over (Order by sum(FTraffic.NbVolRetard) Desc) as classement
from DCompagnie, DTemps, FTraffic
where FTraffic.CodeComp=DCompagnie.CodeComp
and FTraffic.CodeTemps=DTemps.CodeTemps
group by (DCompagnie.CodeComp,DTemps.Annee)
;

/*Classement dense*/
Select DCompagnie.CodeComp,DTemps.Annee,Sum(FTraffic.NbVolRetard) as Nbr,
dense_rank() over (Order by sum(FTraffic.NbVolRetard) Desc) as classement
from DCompagnie, DTemps , FTraffic
where FTraffic.CodeComp=DCompagnie.CodeComp
and FTraffic.CodeTemps=DTemps.CodeTemps
group by (DCompagnie.CodeComp,DTemps.Annee)
;


/*Réponse 8*/
--Donner la répartition cumulative du nombre de vols , par compagnie dans chaque année.

Select DTemps.Annee , DCompagnie.NomComp, sum(FTraffic.NbVol) as SommeNbvol,
  cume_dist() over (partition by DTemps.Annee order by sum(FTraffic.NbVol)) 
    as cum_dist_nb
from  DCompagnie, FTraffic, DTemps
where FTraffic.CodeComp=DCompagnie.CodeComp
and FTraffic.CodeTemps=DTemps.CodeTemps
group by DTemps.Annee , DCompagnie.NomComp
order by DTemps.Annee , DCompagnie.NomComp
;

/*Réponse 9*/
-- Nombre de vols global pour chaque ville de départ, et la segmentation des ville en 4 segments à l’aide de la fonction ntile

Select DAeroportDep.CodeVille,DTemps.Annee,Sum(FTraffic.NbVol) as NbrG,
NTILE(4) OVER(ORDER BY Sum(FTraffic.NbVol) DESC) as ntile4  
from  DAeroportDep,FTraffic,DTemps
where DAeroportDep.NumAerD=FTraffic.NumAerD
and FTraffic.CodeTemps = DTemps.CodeTemps
group by DAeroportDep.CodeVille,DTemps.Annee
;

/*Réponse 10*/
-- Ratio de nombre de vol en retard pour chaque pays de départ, dans chaque année

Select DTemps.Annee , DAeroportDep.NomAerD, sum(FTraffic.NbVolRetard) as NBVolR, 
        sum(sum(FTraffic.NbVolRetard)) over() as TotalNbVolRetard,
        ratio_to_report (sum(FTraffic.NbVolRetard))
          over(partition by DTemps.Annee) as Ratio
from DTemps, DAeroportDep, FTraffic
where FTraffic.NumAerD = DAeroportDep.NumAerD 
and FTraffic.CodeTemps = DTemps.CodeTemps
group by DTemps.Annee , DAeroportDep.NomAerD
;

/*Réponse 11*/
-- Pour chaque année, le pays de départ qui a un nombre de vol en retard maximal

Select  Annee,CodePays, NbrVol
from (select DTemps.Annee,DAeroportDep.CodePays, 
        sum(FTraffic.NbVolRetard) as NbrVol, 
        max (sum(FTraffic.NbVolRetard)) 
          over (partition by DTemps.Annee) as NbrVolMax
      from DAeroportDep,FTraffic,DTemps
where DAeroportDep.NumAerD=FTraffic.NumAerD 
     and DTemps.CodeTemps=FTraffic.CodeTemps
group by DTemps.Annee,DAeroportDep.CodePays)
where NbrVol= NbrVolMax;
