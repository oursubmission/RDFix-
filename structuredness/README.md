# Computing the level of structuredness of an RDF dataset
Implemented based on the concept presented in: https://dl.acm.org/doi/10.1145/1989323.1989340

# How to play with
1) The `structuredness.run` is a binary file to play with the levels of structuredness of a dataset. For example, `./structuredness.run -i /path_to/inputfilename -o 0` will retuern the structuredness of a dataset. Similarly, `./structuredness.run -i /path_to/inputfilename -w /path_to/outputfilename -o 1 -c 0.8 -s 0.25 -r 0.0` will retuern a dataset whose structuredness and size are 80% and 25% of the original file, respectively.


# How to build
1) Set `LD LIBRARY PATH` variable properly referring to the project folder (You may like to take a look at here as well: http://lpsolve.sourceforge.net/5.5/)
2) We used the following flags: `gcc -L. -I. -Werror=vla -Wextra -Wall -Wshadow -Wswitch-default  -fsanitize=address -g -DDEBUG=2 -o ./structuredness.run ./structuredness.c -llpsolve55 -lm




                                

