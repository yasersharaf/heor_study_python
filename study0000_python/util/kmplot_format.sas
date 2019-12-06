%macro SurvivalTemplateRestore;

   %global TitleText0 TitleText1 TitleText2 yOptions xOptions tips
           groups bandopts gridopts blockopts censored censorstr;

   %let TitleText0 = "Probability of persistence";
   %let TitleText1 = &titletext0 " for " STRATUMID;
   %let TitleText2 = &titletext0 "s";         /* plural: Survival Estimates */

   %let yOptions   = label="Persistence"
                     shortlabel="Survival"
                     linearopts=(viewmin=0 viewmax=1
                                 tickvaluelist=(0 .2 .4 .6 .8 1.0));

   %let xOptions   = label ="Days from index date"
                     offsetmin=.05
                     linearopts=(viewmax=MAXTIME tickvaluelist=XTICKVALS
                                 tickvaluefitpolicy=XTICKVALFITPOL);

   %let tips       = rolename=(_tip1=ATRISK _tip2=EVENT) tip=(y x Time _tip1 _tip2);

   %let groups     = group=STRATUM index=STRATUMNUM;

   %let bandopts   = &groups modelname="Survival";

   %let gridopts   = autoalign=(TOPRIGHT BOTTOMLEFT TOP BOTTOM)
                     border=true BackgroundColor=GraphWalls:Color Opaque=true;

   %let blockopts  = repeatedvalues=true valuehalign=start valuefitpolicy=truncate
                     labelposition=left labelattrs=GRAPHVALUETEXT
                     valueattrs=GRAPHDATATEXT(size=7pt) includemissingclass=false;

   %let censored   = markerattrs=(symbol=plus);
   %let censorstr  = "+ Censored";


   %macro SurvivalTemplate;
      proc template;
         define statgraph Stat.Lifetest.Graphics.ProductLimitSurvival;
         dynamic NStrata xName plotAtRisk plotCL plotHW plotEP labelCL
            %if %nrbquote(&censored) ne %then plotCensored;
            labelHW labelEP maxTime xtickVals xtickValFitPol method StratumID
            classAtRisk plotBand plotTest GroupName yMin Transparency SecondTitle
            TestName pValue;
            BeginGraph;

               if (NSTRATA=1)
                  if (EXISTS(STRATUMID))
                     entrytitle &titletext1;
                  else
                     entrytitle &titletext0;
                  endif;
                  if (PLOTATRISK)
                     entrytitle "with Number of Subjects at Risk" / textattrs=
                        GRAPHVALUETEXT;
                  endif;

                  layout overlay / xaxisopts=(&xoptions) yaxisopts=(&yoptions);
                     %singlestratum
                  endlayout;
               endif;

            EndGraph;
         end;
      run;
   %mend;


   %macro SingleStratum;
      if (PLOTHW=1 AND PLOTEP=0)
         bandplot LimitUpper=HW_UCL LimitLower=HW_LCL x=TIME /
            modelname="Survival" fillattrs=GRAPHCONFIDENCE
            name="HW" legendlabel=LABELHW;
      endif;
      if (PLOTHW=0 AND PLOTEP=1)
         bandplot LimitUpper=EP_UCL LimitLower=EP_LCL x=TIME /
            modelname="Survival" fillattrs=GRAPHCONFIDENCE
            name="EP" legendlabel=LABELEP;
      endif;
      if (PLOTHW=1 AND PLOTEP=1)
         bandplot LimitUpper=HW_UCL LimitLower=HW_LCL x=TIME /
            modelname="Survival" fillattrs=GRAPHDATA1 datatransparency=.55
            name="HW" legendlabel=LABELHW;
         bandplot LimitUpper=EP_UCL LimitLower=EP_LCL x=TIME /
            modelname="Survival" fillattrs=GRAPHDATA2
            datatransparency=.55 name="EP" legendlabel=LABELEP;
      endif;
      if (PLOTCL=1)
         if (PLOTHW=1 OR PLOTEP=1)
            bandplot LimitUpper=SDF_UCL LimitLower=SDF_LCL x=TIME /
               modelname="Survival" display=(outline)
               outlineattrs=GRAPHPREDICTIONLIMITS name="CL" legendlabel=LABELCL;
         else
            bandplot LimitUpper=SDF_UCL LimitLower=SDF_LCL x=TIME /
               modelname="Survival" fillattrs=GRAPHCONFIDENCE name="CL"
               legendlabel=LABELCL;
         endif;
      endif;

      stepplot y=SURVIVAL x=TIME / name="Survival" &tips legendlabel="Survival";

      if (PLOTCENSORED=1)
         scatterplot y=CENSORED x=TIME / &censored
            name="Censored" legendlabel="Censored";
      endif;

      if (PLOTCL=1 OR PLOTHW=1 OR PLOTEP=1)
         discretelegend "Censored" "CL" "HW" "EP" / location=outside
            halign=center;
      else
         if (PLOTCENSORED=1)
            discretelegend "Censored" / location=inside
                                        autoalign=(topright bottomleft);
         endif;
      endif;
      if (PLOTATRISK=1)
         innermargin / align=bottom;
            blockplot x=TATRISK block=ATRISK / display=(values) &blockopts;
         endinnermargin;
      endif;
   %mend;


%SurvivalTemplate
%mend;

%SurvivalTemplateRestore
%SurvivalTemplate;
