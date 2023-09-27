from django.shortcuts import render
from .search_call import search

# Create your views here.
def search_index(request):
    results = []
    search_term = ""
    context = {'results': {}, 'count': 0, 'search_term': 'empty'}
    if request.GET.get('keyword'):
        search_term = request.GET['keyword']
        results = search(query = search_term, topN = 20)
        print(results)
        context = {'results': results[1], 'count': results[0], 'search_term': search_term}
    return render(request,  'index.html',  context)
