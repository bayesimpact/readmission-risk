"""Helper methods related to plotting."""

from IPython.display import HTML

import matplotlib.pylab as plt

from sklearn.metrics import auc, roc_curve


def create_roc_curve(y_true, y_pred, title):
    """Draw an ROC curve."""
    fig, ax = plt.subplots()
    if not isinstance(y_pred, list):
        y_pred = [y_pred]
        title = [title]
    for i, pred in enumerate(y_pred):
        fpr, tpr, thresholds = roc_curve(y_true, pred)
        ax.plot(fpr, tpr, label='ROC curve of %s (area = %0.2f)' % (title[i], auc(fpr, tpr)))

    ax.plot([0, 1], [0, 1], 'k--')
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel('False Positive Rate')
    ax.set_ylabel('True Positive Rate')
    ax.legend(loc="lower right")
    plt.show()


def embed_map(map, path="map.html", width='50%'):
    """
    Embed a linked iframe to the map into the IPython notebook.

    Note: this method will not capture the source of the map into the notebook.
    This method should work for all maps (as long as they use relative urls).
    """
    map.create_map(path=path)
    ifr = '<iframe src="files/{path}" style="width: {width}; height: 510px; border: none"></iframe>'
    return HTML(ifr.format(path=path, width=width))
