#!/bin/bash
rm -rf *.aux
rm -rf *.bbl
rm -rf *.blg
rm -rf *.pdf
rm -rf *.log
echo "preparing latex"
python gdoc2latex.py "https://docs.google.com/document/d/1LGAqWDRQvSyFODht4M-uu1j-Sx0abpLjIRrON53J-KQ/edit" > barista.tex
echo "latexing"
pdflatex barista.tex
echo "creating bibtex"
bibtex barista
echo "creating PDF"
pdflatex barista.tex
pdflatex barista.tex

echo "DONE"
