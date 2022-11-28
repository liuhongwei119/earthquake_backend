"""empty message

Revision ID: 512585b9d9eb
Revises: 83ef8d5f608c
Create Date: 2022-11-28 01:27:09.844585

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '512585b9d9eb'
down_revision = '83ef8d5f608c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('earth_curve', schema=None) as batch_op:
        batch_op.create_unique_constraint(None, ['curve_id'])

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('earth_curve', schema=None) as batch_op:
        batch_op.drop_constraint(None, type_='unique')

    # ### end Alembic commands ###
