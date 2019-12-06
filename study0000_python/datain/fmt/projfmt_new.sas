/*format macro */

%macro format(value,format);
    %if %datatyp(&value)=CHAR
        %then %sysfunc(putc(&value,&format));
        %else %left(%qsysfunc(putn(&value,&format)));
%mend format;
