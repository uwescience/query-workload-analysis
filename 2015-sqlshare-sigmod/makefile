all:
	pdflatex --shell-escape paper.tex
	bibtex paper
	bibtex paper
	pdflatex --shell-escape paper.tex
	pdflatex --shell-escape paper.tex
clean:
	rm *.bbl *.log *.pyg *.out *.aux *.blg *.dvi *.fls figures/*eps-converted-to.pdf
