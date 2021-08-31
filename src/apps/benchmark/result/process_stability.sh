#!/usr/bin/bash -f


awk -F "s]|ops/s]" 'BEGIN{t=1;qps=0;} {if(NR>=$start && NR <=$end) {split($1,a,"["); sub(/^[[:blank:]]*/,"",a[2]); lt=a[2]; split($2,b,"["); sub(/^[[:blank:]]*/,"",b[2]); q=b[2]; if(t==lt){qps+=q;} else { print t,qps;t=lt;qps=0;} } }' 