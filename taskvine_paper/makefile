all: 0cocurrent.png 25cocurrent.png 3cocurrent.png nominitask.png minitask.png nopeerWWCold.png peerWWHot.png workershared.png workerpeer.png topefttaskview.png topeftworkerview.png  colmenataskview.png colmenaworkerview.png gmotaskview.png gmoworkerview.png topefttaskview-annotated.png topefttemptaskview.png topefttempworkerview.png


0cocurrent.png: transfer_rate/plot_rate transfer_rate/updated_manager
	transfer_rate/plot_rate  --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend none transfer_rate/updated_manager 0cocurrent.png
25cocurrent.png: transfer_rate/plot_rate transfer_rate/updated_25_concurrent
	transfer_rate/plot_rate  --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend none transfer_rate/updated_25_concurrent 25cocurrent.png
3cocurrent.png: transfer_rate/plot_rate transfer_rate/updated_3_concurrent
	transfer_rate/plot_rate --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend "lower right" transfer_rate/updated_3_concurrent 3cocurrent.png



nominitask.png: minitask/plot_poncho minitask/nominitask
	minitask/plot_poncho --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend none minitask/nominitask nominitask.png
minitask.png: minitask/plot_poncho minitask/minitask
	minitask/plot_poncho --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend "lower right" minitask/minitask minitask.png
nopeerWWCold.png: blast/plot_blast blast/cold
	blast/plot_blast  --origin dispatched-first-task --dpi 150 --title " " --legend none  blast/cold nopeerWWCold.png
peerWWHot.png: blast/plot_blast blast/hot
	blast/plot_blast  --origin dispatched-first-task --dpi 150 --title " " --legend "lower right" blast/hot peerWWHot.png
workershared.png: peer/plot_peer peer/bigstat_5k
	peer/plot_peer --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend none peer/bigstat_5k workershared.png
workerpeer.png: peer/plot_peer peer/transfer_bigstat_5k_tar
	peer/plot_peer --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend "lower right" peer/transfer_bigstat_5k_tar workerpeer.png




topefttaskview.png: topeft/plot_topeft topeft/topeft
	topeft/plot_topeft --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend "lower right" topeft/topeft topefttaskview.png
topefttaskview-annotated.png: topefttaskview.png
	convert -pointsize 60 -annotate +315+1030 "<-Preprocessing" -annotate +650+550 "<-Processing Tasks" -annotate +1100+300 "<-Accumulation Tasks" topefttaskview.png topefttaskview-annotated.png
topeftworkerview.png: topeft/plot_topeft topeft/topeft
	topeft/plot_topeft --origin dispatched-first-task --dpi 150 --title " " --legend none topeft/topeft topeftworkerview.png
topefttemptaskview.png: topeft/plot_topeft topeft/topefttemp
	topeft/plot_topeft --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend none topeft/topefttemp topefttemptaskview.png
topefttempworkerview.png: topeft/plot_topeft topeft/topefttemp
	topeft/plot_topeft --origin dispatched-first-task --dpi 150 --title " " --legend none topeft/topefttemp topefttempworkerview.png


colmenataskview.png: colmena/plot_colmena colmena/transactions_peer_enabled
	colmena/plot_colmena --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend none colmena/transactions_peer_enabled colmenataskview.png
	convert -pointsize 60 -annotate +800+950 "<-Inference Tasks" -annotate +1200+700 "<-Simulation Tasks" colmenataskview.png colmenataskview.png
colmenaworkerview.png: colmena/plot_colmena colmena/transactions_peer_enabled
	colmena/plot_colmena --origin dispatched-first-task --dpi 150 --title " " --legend none colmena/transactions_peer_enabled colmenaworkerview.png

gmotaskview.png: gmo/plot_gmo gmo/transactions
	gmo/plot_gmo --mode tasks --origin dispatched-first-task --dpi 150 --title " " --legend none gmo/transactions gmotaskview.png
	convert -pointsize 60 -annotate +300+100 "Library Tasks->" -annotate +525+600 "Function Calls->" gmotaskview.png gmotaskview.png
gmoworkerview.png: gmo/plot_gmo gmo/transactions
	gmo/plot_gmo --origin dispatched-first-task --dpi 150 --title " " --legend "lower right" gmo/transactions gmoworkerview.png


clean: 
	rm *.png
